from __future__ import annotations

from datetime import datetime
from functools import wraps
from typing import Any, Awaitable, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from bot.games import (
    challenge_summary,
    claim_and_activate_match,
    create_challenge,
    settle_match,
    start_dice_game,
    start_football_game,
    start_chess_game,
    start_mlbb_game,
)
from bot.keyboards import accept_challenge_keyboard, deposit_keyboard, games_keyboard, main_menu_keyboard
from bot.payments import create_withdrawal_request, notify_deposit_prompt, transfer_tip
from config import logger, settings
from db.models import (
    ensure_user,
    get_active_mlbb_match_for_user,
    get_match,
    get_settings_doc,
    get_user,
    get_user_by_username,
    list_matches_for_user,
    list_transactions_for_user,
    top_wagerers,
    update_match,
)
from utils import (
    ANTI_CHEAT_WARNING,
    PRIVATE_ONLY_TEXT,
    bot_private_link,
    display_name,
    format_amount,
    is_private_chat,
    parse_user_reference,
    private_only_markup,
    is_rate_limited,
    utcnow,
    win_rate,
)

UserHandler = Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]


def sandbox_note() -> str:
    return "🧪 Sandbox mode: TON economy is disabled."


PROFILE_TIERS: list[dict[str, Any]] = [
    {"name": "Apprentice", "min": 0.0, "daily": 0.05, "weekly": 0.02, "monthly": 0.03},
    {"name": "Contender", "min": 250.0, "daily": 0.08, "weekly": 0.03, "monthly": 0.05},
    {"name": "Hustler", "min": 1000.0, "daily": 0.12, "weekly": 0.05, "monthly": 0.08},
    {"name": "High Roller", "min": 5000.0, "daily": 0.18, "weekly": 0.075, "monthly": 0.1},
    {"name": "Legend", "min": 20000.0, "daily": 0.24, "weekly": 0.09, "monthly": 0.125},
]


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _progress_bar(percent: float, width: int = 16) -> str:
    clamped = max(0.0, min(100.0, percent))
    filled = round((clamped / 100.0) * width)
    return f"{'█' * filled}{'░' * (width - filled)}"


def _format_joined(user: dict[str, Any]) -> str:
    joined = user.get("joined_at")
    if isinstance(joined, datetime):
        return joined.strftime("%d %b %Y")
    return "Unknown"


def _profile_tier_info(total_wagered: float) -> tuple[dict[str, Any], dict[str, Any] | None, float]:
    current = PROFILE_TIERS[0]
    next_tier: dict[str, Any] | None = None
    for idx, tier in enumerate(PROFILE_TIERS):
        if total_wagered >= float(tier["min"]):
            current = tier
            if idx + 1 < len(PROFILE_TIERS):
                next_tier = PROFILE_TIERS[idx + 1]
        else:
            break
    if not next_tier:
        return current, None, 100.0
    span = float(next_tier["min"]) - float(current["min"])
    done = max(0.0, total_wagered - float(current["min"]))
    percent = 100.0 if span <= 0 else (done / span) * 100.0
    return current, next_tier, max(0.0, min(100.0, percent))


def _profile_card(user: dict[str, Any]) -> str:
    total_wagered = _as_float(user.get("total_wagered"))
    total_wins = int(user.get("total_wins") or 0)
    total_losses = int(user.get("total_losses") or 0)
    games_played = int(user.get("games_played") or 0)
    win_pct = win_rate(total_wins, total_losses)
    current, next_tier, progress = _profile_tier_info(total_wagered)
    next_need = 0.0 if not next_tier else max(0.0, float(next_tier["min"]) - total_wagered)
    joined = _format_joined(user)
    username = user.get("username")
    handle = f"@{username}" if username else "No username"
    tier_line = f"{current['name']}" if not next_tier else f"{current['name']} ➜ {next_tier['name']}"
    bar = _progress_bar(progress)
    lines = [
        "🪪 Profile",
        f"👤 {display_name(user)} · {handle}",
        f"🆔 `{user['_id']}` · 📅 {joined}",
        "",
        f"💰 Balance: {format_amount(_as_float(user.get('balance')))} TON",
        f"🏅 Level: {tier_line}",
        f"📈 Wagered: {format_amount(total_wagered)} TON",
        f"📊 Progress: {bar} {round(progress, 1)}%",
        f"🎁 Perks: D {current['daily']}% · W {current['weekly']}% · M {current['monthly']}%",
        f"🎮 Games: {games_played} · 🏆 Win rate: {win_pct}%",
        f"📉 P/L: {format_amount(_as_float(user.get('total_profit')))} TON",
        f"👑 VIP: {'Yes' if user.get('is_vip') else 'No'} · ⚔️ MLBB: {user.get('mlbb_id') or 'Not set'}",
    ]
    lines.append(
        f"🚀 Next tier in {format_amount(next_need)} TON" if next_tier else "🚀 Max tier reached"
    )
    return "\n".join(lines)


def guard_handler(func: UserHandler) -> UserHandler:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            user_id = update.effective_user.id if update.effective_user else None
            chat_id = update.effective_chat.id if update.effective_chat else None
            if is_rate_limited("user_cmd", user_id):
                if update.effective_message:
                    await update.effective_message.reply_text("⏳ Too many requests. Please slow down a bit.")
                return
            if is_rate_limited("chat_cmd", chat_id):
                return
            await func(update, context)
        except PermissionError as exc:
            if update.effective_message:
                await update.effective_message.reply_text(str(exc))
        except Exception as exc:
            logger.exception("Handler %s failed: %s", func.__name__, exc)
            if update.effective_message:
                await update.effective_message.reply_text("⚠️ Something went wrong. Please try again.")

    return wrapper  # type: ignore[return-value]


async def ensure_current_user(update: Update) -> dict[str, Any]:
    user = await ensure_user(
        update.effective_user.id,
        update.effective_user.username,
        update.effective_user.first_name,
    )
    if not user or user.get("is_banned"):
        raise PermissionError("🚫 You are banned from using this bot.")
    return user


async def require_private(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if is_private_chat(update):
        return True
    await update.effective_message.reply_text(
        PRIVATE_ONLY_TEXT.format(link=bot_private_link(context.bot.username)),
        reply_markup=private_only_markup(context.bot.username),
    )
    return False


def format_leaderboard(rows: list[dict[str, Any]]) -> str:
    lines = ["🏆 Leaderboard"]
    for idx, row in enumerate(rows, start=1):
        badge = " 👑" if row.get("is_vip") else ""
        lines.append(f"{idx}. {display_name(row)}{badge} — {format_amount(float(row['total_wagered']))} TON")
    return "\n".join(lines)


@guard_handler
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"✨ Welcome back, {display_name(user)}",
                "Use the menu below to manage wallet, games, and profile.",
                "Quick: /play /wallet /me /logs /top /fund /cashout",
            ]
        ),
        reply_markup=main_menu_keyboard(user_id=update.effective_user.id if update.effective_user else None),
    )


@guard_handler
async def deposit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await ensure_current_user(update)
    if not await require_private(update, context):
        return
    if settings.sandbox_mode:
        await update.effective_message.reply_text("🧪 Deposits are disabled in sandbox mode.")
        return
    if not settings.ton_enabled:
        await update.effective_message.reply_text("⚠️ TON deposit logic is temporarily disabled in development mode.")
        return
    await update.effective_message.reply_text(
        "💳 Choose a deposit method.\nInclude your Telegram user ID as memo/comment.",
        reply_markup=deposit_keyboard(),
    )


@guard_handler
async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if not await require_private(update, context):
        return
    if settings.sandbox_mode:
        await update.effective_message.reply_text("🧪 Withdrawals are disabled in sandbox mode.")
        return
    if not settings.ton_enabled:
        await update.effective_message.reply_text("⚠️ TON withdrawal logic is temporarily disabled in development mode.")
        return
    if len(context.args) != 2:
        await update.effective_message.reply_text(
            "ℹ️ Usage: `/cashout <amount> <address>`\nExample: `/cashout 10 UQ...abc`"
        )
        return
    try:
        amount = round(float(context.args[0]), 8)
    except ValueError:
        await update.effective_message.reply_text("❌ Amount must be numeric.")
        return
    if amount <= 0:
        await update.effective_message.reply_text("❌ Amount must be greater than zero.")
        return
    settings_doc = await get_settings_doc()
    fee_percent = float(settings_doc.get("withdrawal_fee_percent", settings.withdrawal_fee_percent))
    fee = round(amount * (fee_percent / 100), 8)
    total_cost = round(amount + fee, 8)
    if float(user["balance"]) < total_cost:
        await update.effective_message.reply_text(f"❌ Insufficient balance. Required: {format_amount(total_cost)} TON")
        return
    from bot.keyboards import withdrawal_admin_keyboard
    try:
        withdrawal_id, _, net_amount = await create_withdrawal_request(update.effective_user.id, amount, context.args[1].strip())
    except ValueError:
        await update.effective_message.reply_text("❌ Insufficient balance. Balance changed before reservation.")
        return
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"✅ Withdrawal request created: `{withdrawal_id}`",
                f"💸 Gross: {format_amount(amount)} TON",
                f"💰 Net: {format_amount(net_amount)} TON",
                "📨 Admins have been notified.",
            ]
        )
    )
    for admin_id in settings.admin_ids:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"⚠️ New withdrawal request\n"
                    f"ID: `{withdrawal_id}`\n"
                    f"User: {display_name(user)} (`{user['_id']}`)\n"
                    f"Amount: {format_amount(amount)} TON\n"
                    f"Address: `{context.args[1].strip()}`"
                ),
                reply_markup=withdrawal_admin_keyboard(withdrawal_id),
            )
        except Exception:
            pass


@guard_handler
async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if not await require_private(update, context):
        return
    text = f"💰 Balance: {format_amount(float(user['balance']))} TON"
    if settings.sandbox_mode:
        text = f"{text}\n{sandbox_note()}"
    await update.effective_message.reply_text(text)


@guard_handler
async def tip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if len(context.args) != 2:
        await update.effective_message.reply_text(
            "ℹ️ Usage: `/tip <@username|user_id> <amount>`\nExample: `/tip @rahul 2`"
        )
        return
    target_raw, is_username = parse_user_reference(context.args[0])
    try:
        amount = round(float(context.args[1]), 8)
    except ValueError:
        await update.effective_message.reply_text("❌ Amount must be numeric.")
        return
    if amount < 0.1:
        await update.effective_message.reply_text("❌ Minimum tip is 0.1 TON.")
        return
    recipient = await get_user_by_username(target_raw) if is_username else await get_user(target_raw)
    if not recipient or recipient.get("is_banned"):
        await update.effective_message.reply_text("❌ Recipient is unavailable.")
        return
    if str(recipient["_id"]) == str(user["_id"]):
        await update.effective_message.reply_text("❌ You cannot tip yourself.")
        return
    if settings.sandbox_mode:
        await update.effective_message.reply_text(
            f"✅ Sent {format_amount(amount)} TON to {display_name(recipient)}.\n{sandbox_note()}"
        )
        try:
            await context.bot.send_message(
                chat_id=int(recipient["_id"]),
                text=(
                    f"You received a tip of {format_amount(amount)} TON from {display_name(user)}.\n"
                    f"{sandbox_note()}"
                ),
            )
        except Exception:
            pass
        return
    tip_key = f"tip_transfer:{update.effective_chat.id}:{update.effective_message.message_id}:{user['_id']}:{recipient['_id']}"
    if not await transfer_tip(user["_id"], recipient["_id"], amount, idempotency_key=tip_key):
        await update.effective_message.reply_text("❌ Insufficient balance.")
        return
    await update.effective_message.reply_text(f"✅ Sent {format_amount(amount)} TON to {display_name(recipient)}.")
    try:
        await context.bot.send_message(
            chat_id=int(recipient["_id"]),
            text=f"You received a tip of {format_amount(amount)} TON from {display_name(user)}.",
        )
    except Exception:
        pass


@guard_handler
async def challenge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args

    # ── Usage hint ──────────────────────────────────────────────────────────────
    if not args or len(args) < 2:
        await update.effective_message.reply_text(
            "ℹ️ Usage: `/play <amount> <game> [mode] [count]`\n"
            "Example: `/play 10 dice normal 2`"
        )
        return

    # ── Parse amount ─────────────────────────────────────────────────────────────
    try:
        amount = round(float(args[0]), 8)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid amount. Must be a positive number.")
        return

    # ── Parse game ───────────────────────────────────────────────────────────────
    game = args[1].strip().lower()
    if game not in {"dice", "football", "chess", "mlbb"}:
        await update.effective_message.reply_text(
            "❌ Invalid game.\nChoose from: dice, football, chess, mlbb"
        )
        return

    # ── Parse mode + dice_count (dice/football only) ─────────────────────────────
    mode = "normal"
    dice_count = 1

    if game in {"dice", "football"}:
        if len(args) >= 3:
            parsed_mode = args[2].strip().lower()
            if parsed_mode not in {"normal", "crazy"}:
                await update.effective_message.reply_text(
                    f"❌ Invalid mode '{parsed_mode}'.\n"
                    "Use: normal or crazy.\n"
                    f"Example: `/play {format_amount(amount)} {game} normal 2`"
                )
                return
            mode = parsed_mode
        if len(args) >= 4:
            try:
                dice_count = int(args[3])
                if dice_count not in {1, 2, 3}:
                    raise ValueError
            except ValueError:
                await update.effective_message.reply_text(
                    "❌ Dice/shot count must be 1, 2, or 3."
                )
                return
    # chess and mlbb: mode=None, dice_count=None — extra args silently ignored
    else:
        mode = None
        dice_count = None

    # ── Auth + balance check ─────────────────────────────────────────────────────
    user = await ensure_current_user(update)

    if game == "mlbb" and not user.get("mlbb_id"):
        await update.effective_message.reply_text(
            "❌ Set your MLBB ID first with /setmlbb <mlbb_id>"
        )
        return

    if (not settings.sandbox_mode) and float(user.get("balance", 0.0)) < amount:
        await update.effective_message.reply_text(
            f"❌ Insufficient balance.\n"
            f"Your balance: {format_amount(float(user.get('balance', 0.0)))} TON\n"
            f"Required: {format_amount(amount)} TON"
        )
        return

    # ── Create challenge (reserves balance atomically) ───────────────────────────
    try:
        match = await create_challenge(
            user, amount, game, mode, dice_count,
            update.effective_chat.id
        )
    except ValueError:
        await update.effective_message.reply_text(
            "❌ Could not reserve balance. Try again."
        )
        return

    # ── Post challenge card ──────────────────────────────────────────────────────
    try:
        summary_text = challenge_summary(match, user)
    except Exception:
        logger.exception("challenge_summary failed for match_id=%s", match.get("_id"))
        summary_text = (
            "PvP Challenge\n"
            f"Match ID: `{match['_id']}`\n"
            f"Bet: {format_amount(float(match['amount']))} TON"
        )
    await update.effective_message.reply_text(
        summary_text,
        reply_markup=accept_challenge_keyboard(match["_id"]),
    )


@guard_handler
async def accept_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    match_id: str | None = None,
) -> None:
    user = await ensure_current_user(update)
    target_match_id = match_id or (context.args[0] if context.args else "")

    if not target_match_id:
        await update.effective_message.reply_text("ℹ️ Usage: `/accept <match_id>`\nExample: `/accept ab12cd34`")
        return

    match = await get_match(target_match_id)
    if not match or match["status"] != "pending":
        await update.effective_message.reply_text("❌ Match already taken, cancelled, or unavailable.")
        return

    if str(match["challenger_id"]) == str(user["_id"]):
        await update.effective_message.reply_text("❌ You cannot accept your own challenge.")
        return

    if match["game"] == "mlbb" and not user.get("mlbb_id"):
        await update.effective_message.reply_text(
            "❌ Set your MLBB ID first with /setmlbb <mlbb_id>"
        )
        return

    # ── Atomic claim ─────────────────────────────────────────────────────────────
    try:
        active_match = await claim_and_activate_match(match, user)
    except ValueError:
        await update.effective_message.reply_text("❌ Match already taken or insufficient balance.")
        return
    if not active_match:
        await update.effective_message.reply_text("❌ Match already taken. Try another.")
        return

    # ── Route to correct PvP game start ──────────────────────────────────────────
    game = active_match["game"]
    chat_id = update.effective_chat.id

    if game == "dice":
        await start_dice_game(context, active_match, chat_id)

    elif game == "football":
        await start_football_game(context, active_match, chat_id)

    elif game == "chess":
        await start_chess_game(context, active_match, chat_id)

    elif game == "mlbb":
        await start_mlbb_game(context, active_match, chat_id)


@guard_handler
async def result_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if len(context.args) != 2:
        await update.effective_message.reply_text(
            "ℹ️ Usage: `/result <match_id> <win|lose>`\nExample: `/result ab12cd34 win`"
        )
        return
    match_id = context.args[0]
    result = context.args[1].lower()
    if result not in {"win", "lose"}:
        await update.effective_message.reply_text("❌ Result must be `win` or `lose`.")
        return
    match = await get_active_mlbb_match_for_user(user["_id"], match_id)
    if not match:
        await update.effective_message.reply_text("❌ No active MLBB match found.")
        return

    # Determine which field to update
    is_challenger = str(match["challenger_id"]) == str(user["_id"])
    field = "challenger_result" if is_challenger else "opponent_result"

    # Check if already reported
    if match.get(field):
        await update.effective_message.reply_text("❌ You already reported a result for this match.")
        return

    match = await update_match(match_id, {field: result})
    if not match:
        await update.effective_message.reply_text("❌ Failed to update result.")
        return

    # Wait for both
    if not match.get("challenger_result") or not match.get("opponent_result"):
        await update.effective_message.reply_text("✅ Result recorded. Waiting for the other player.")
        return

    # Both reported — check agreement
    c_result = match["challenger_result"]
    o_result = match["opponent_result"]

    # Conflict: both claim win or both claim lose
    if c_result == o_result:
        await update_match(match_id, {"status": "disputed"})
        challenger = await get_user(match["challenger_id"])
        opponent = await get_user(match["opponent_id"])
        admin_text = (
            f"⚠️ Disputed MLBB match `{match_id}`\n\n"
            f"Challenger: {display_name(challenger)}\n"
            f"- Telegram ID: `{match['challenger_id']}`\n"
            f"- MLBB ID: `{(challenger or {}).get('mlbb_id') or 'Not set'}`\n"
            f"- Submitted: `{c_result}`\n\n"
            f"Opponent: {display_name(opponent)}\n"
            f"- Telegram ID: `{match['opponent_id']}`\n"
            f"- MLBB ID: `{(opponent or {}).get('mlbb_id') or 'Not set'}`\n"
            f"- Submitted: `{o_result}`\n\n"
            f"Resolve with: /resolve {match_id} <winner_user_id>"
        )
        for admin_id in settings.admin_ids:
            try:
                await context.bot.send_message(chat_id=admin_id, text=admin_text)
            except Exception:
                pass
        await update.effective_message.reply_text(
            f"⚠️ Match `{match_id}` is disputed. Admins have been notified.\n{ANTI_CHEAT_WARNING}"
        )
        return

    # Clear winner
    winner_id = match["challenger_id"] if c_result == "win" else match["opponent_id"]
    settled, payout, fee = await settle_match(match, str(winner_id))
    winner = await get_user(winner_id)
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"🎮 MLBB match `{settled['_id']}` resolved!",
                f"🏆 Winner: {display_name(winner)}",
                f"💰 Payout: {format_amount(payout)} TON",
                f"🏦 House fee: {format_amount(fee)} TON",
                ANTI_CHEAT_WARNING,
            ]
        )
    )


@guard_handler
async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    text = _profile_card(user)
    if settings.sandbox_mode:
        text = f"{text}\n{sandbox_note()}"
    await update.effective_message.reply_text(text)


@guard_handler
async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if not await require_private(update, context):
        return
    transactions = await list_transactions_for_user(user["_id"], limit=10)
    matches = await list_matches_for_user(user["_id"], limit=10)
    tx_lines = (
        [f"{tx['type']} {format_amount(float(tx['amount']))} TON [{tx['status']}]" for tx in transactions]
        or ["No transactions yet."]
    )
    match_lines = (
        [
            f"{m['_id']} {m['game']} {format_amount(float(m['amount']))} TON [{m['status']}]"
            for m in matches
        ]
        or ["No matches yet."]
    )
    text = (
        "📜 Last 10 transactions\n"
        + "\n".join(tx_lines)
        + "\n\n🎯 Last 10 matches\n"
        + "\n".join(match_lines)
    )
    if settings.sandbox_mode:
        text = f"{text}\n\n{sandbox_note()}"
    await update.effective_message.reply_text(text)


@guard_handler
async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await ensure_current_user(update)
    rows = await top_wagerers(limit=10)
    await update.effective_message.reply_text(format_leaderboard(rows))


@guard_handler
async def setmlbb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if not await require_private(update, context):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("ℹ️ Usage: `/setmlbb <mlbb_id>`\nExample: `/setmlbb 8204763246`")
        return
    from db.mongo import get_db

    db = await get_db()
    await db.users.update_one(
        {"_id": str(user["_id"])},
        {"$set": {"mlbb_id": context.args[0].strip(), "mlbb_verified": False, "last_active": utcnow()}},
    )
    await update.effective_message.reply_text("✅ MLBB ID saved successfully.")


@guard_handler
async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    action = query.data

    if action == "menu:balance":
        await balance_command(update, context)
    elif action == "menu:games":
        await query.message.reply_text(
            "🎮 Start a game with `/play`.\nExample: `/play 10 dice normal 2`",
            reply_markup=games_keyboard(),
        )
    elif action == "menu:deposit":
        await deposit_command(update, context)
    elif action == "menu:withdraw":
        await query.message.reply_text("💸 Use `/cashout <amount> <address>` (legacy: `/withdraw ...`)")
    elif action == "menu:profile":
        await profile_command(update, context)
    elif action == "menu:history":
        await history_command(update, context)
    elif action == "menu:leaderboard":
        await leaderboard_command(update, context)
    elif action == "menu:tip":
        await query.message.reply_text("🎁 Use `/tip <@username|user_id> <amount>`")
    elif action == "menu:admin":
        if not is_private_chat(update):
            await query.message.reply_text("🔒 Admin panel works in private chat only.")
            return
        if update.effective_user.id not in settings.admin_ids:
            await query.message.reply_text("🚫 Unauthorized.")
            return
        await query.message.reply_text(
            "\n".join(
                [
                    "🛡️ Admin commands:",
                    "/add_balance <user_id> <amount> [reason]",
                    "/deduct_balance <user_id> <amount> [reason]",
                    "/approve_withdrawal <withdrawal_id>",
                    "/reject_withdrawal <withdrawal_id> [reason]",
                    "/approve_deposit <user_id> <amount> <crypto>",
                    "/resolve <match_id> <winner_user_id>",
                    "/admin_stats",
                    "/wager_report",
                    "/admin_user <user_id>",
                    "/admin_matches",
                    "/admin_ban <user_id>",
                    "/admin_unban <user_id>",
                    "/set_fee <percent>",
                    "/set_min_wager <amount>",
                    "/set_deposit_address <TON|USDT_BEP20|SOL> <address>",
                    "/admin_refund <match_id>",
                    "/admin_balance",
                ]
            )
        )
    elif action.startswith("deposit:"):
        await notify_deposit_prompt(update, context, action.split(":", 1)[1])
    elif action.startswith("games:"):
        # Per-game correct usage hint
        selected = action.split(":", 1)[1]
        if selected in ("dice", "football"):
            hint = f"/play <amount> {selected} [normal|crazy] [1|2|3]"
        elif selected == "chess":
            hint = "/play <amount> chess"
        elif selected == "mlbb":
            hint = "/play <amount> mlbb"
        else:
            hint = f"/play <amount> {selected}"
        await query.message.reply_text(f"🎮 {selected.title()} ready.\nExample: `{hint}`")


@guard_handler
async def accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    match_id = query.data.split(":", 1)[1]
    await accept_command(update, context, match_id=match_id)


@guard_handler
async def fallback_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if not is_private_chat(update):
        return
    text = (update.effective_message.text or "").strip()
    if len(text) < 10 or text.startswith("/"):
        return
    for admin_id in settings.admin_ids:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"📥 Possible manual deposit evidence\n"
                    f"User: {display_name(user)} (`{user['_id']}`)\n"
                    f"Message: {text}\n"
                    f"Approve with /approve_deposit {user['_id']} <amount> <crypto>"
                ),
            )
        except Exception:
            pass
    await update.effective_message.reply_text(
        "✅ Your message was forwarded to admins for manual deposit review."
    )
