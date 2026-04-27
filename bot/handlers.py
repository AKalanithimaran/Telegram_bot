from __future__ import annotations

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
)

UserHandler = Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]


def sandbox_note() -> str:
    return "Sandbox mode: TON economy is disabled."


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
    is_admin_user = update.effective_user.id in settings.admin_ids
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"Welcome, {display_name(user)}",
                "Use the menu below to access balance, games, deposits, withdrawals, history, and leaderboard.",
            ]
        ),
        reply_markup=main_menu_keyboard(is_admin=is_admin_user),
    )


@guard_handler
async def deposit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await ensure_current_user(update)
    if not await require_private(update, context):
        return
    if settings.sandbox_mode:
        await update.effective_message.reply_text("Deposit is disabled in sandbox mode.")
        return
    if not settings.ton_enabled:
        await update.effective_message.reply_text(
            "TON deposit logic is temporarily disabled in development mode."
        )
        return
    await update.effective_message.reply_text(
        "Choose a deposit method. Include your Telegram user ID as memo/comment.",
        reply_markup=deposit_keyboard(),
    )


@guard_handler
async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if not await require_private(update, context):
        return
    if settings.sandbox_mode:
        await update.effective_message.reply_text("Withdrawal is disabled in sandbox mode.")
        return
    if not settings.ton_enabled:
        await update.effective_message.reply_text(
            "TON withdrawal logic is temporarily disabled in development mode."
        )
        return
    if len(context.args) != 2:
        await update.effective_message.reply_text("Usage: /withdraw <amount> <address>")
        return
    try:
        amount = round(float(context.args[0]), 8)
    except ValueError:
        await update.effective_message.reply_text("Amount must be numeric.")
        return
    if amount <= 0:
        await update.effective_message.reply_text("Amount must be greater than zero.")
        return
    settings_doc = await get_settings_doc()
    fee_percent = float(settings_doc.get("withdrawal_fee_percent", settings.withdrawal_fee_percent))
    fee = round(amount * (fee_percent / 100), 8)
    total_cost = round(amount + fee, 8)
    if float(user["balance"]) < total_cost:
        await update.effective_message.reply_text(f"Insufficient balance. Required: {format_amount(total_cost)} TON")
        return
    from bot.keyboards import withdrawal_admin_keyboard
    try:
        withdrawal_id, _, net_amount = await create_withdrawal_request(update.effective_user.id, amount, context.args[1].strip())
    except ValueError:
        await update.effective_message.reply_text("Insufficient balance. Balance changed before reservation.")
        return
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"Withdrawal request created: `{withdrawal_id}`",
                f"Gross: {format_amount(amount)} TON",
                f"Net: {format_amount(net_amount)} TON",
                "Admins have been notified.",
            ]
        )
    )
    for admin_id in settings.admin_ids:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"New withdrawal request\n"
                    f"ID: `{withdrawal_id}`\n"
                    f"User: {display_name(user)} ({user['_id']})\n"
                    f"Amount: {format_amount(amount)} TON\n"
                    f"Address: {context.args[1].strip()}"
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
        await update.effective_message.reply_text("Usage: /tip <@username|user_id> <amount>")
        return
    target_raw, is_username = parse_user_reference(context.args[0])
    try:
        amount = round(float(context.args[1]), 8)
    except ValueError:
        await update.effective_message.reply_text("Amount must be numeric.")
        return
    if amount < 0.1:
        await update.effective_message.reply_text("Minimum tip is 0.1 TON.")
        return
    recipient = await get_user_by_username(target_raw) if is_username else await get_user(target_raw)
    if not recipient or recipient.get("is_banned"):
        await update.effective_message.reply_text("Recipient is unavailable.")
        return
    if str(recipient["_id"]) == str(user["_id"]):
        await update.effective_message.reply_text("You cannot tip yourself.")
        return
    if settings.sandbox_mode:
        await update.effective_message.reply_text(
            f"Sent {format_amount(amount)} TON to {display_name(recipient)}.\n{sandbox_note()}"
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
        await update.effective_message.reply_text("Insufficient balance.")
        return
    await update.effective_message.reply_text(f"Sent {format_amount(amount)} TON to {display_name(recipient)}.")
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
            "Usage:\n"
            "/challenge <amount> dice [normal|crazy] [1|2|3]\n"
            "/challenge <amount> football [normal|crazy] [1|2|3]\n"
            "/challenge <amount> chess\n"
            "/challenge <amount> mlbb"
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
            "❌ Invalid game.\n"
            "Choose from: dice, football, chess, mlbb"
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
                    f"Use: normal or crazy\n"
                    f"Example: /challenge {format_amount(amount)} {game} normal 2"
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
    await update.effective_message.reply_text(
        challenge_summary(match, user),
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
        await update.effective_message.reply_text("Usage: /accept <match_id>")
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
        await update.effective_message.reply_text("Usage: /result <match_id> <win|lose>")
        return
    match_id = context.args[0]
    result = context.args[1].lower()
    if result not in {"win", "lose"}:
        await update.effective_message.reply_text("Result must be win or lose.")
        return
    match = await get_active_mlbb_match_for_user(user["_id"], match_id)
    if not match:
        await update.effective_message.reply_text("No active MLBB match found.")
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
        await update.effective_message.reply_text("Failed to update result.")
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
        for admin_id in settings.admin_ids:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        f"⚠️ Disputed MLBB match `{match_id}`\n"
                        f"Both players reported the same result.\n"
                        f"Resolve with: /resolve {match_id} <winner_user_id>"
                    ),
                )
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
    lines = [
        f"👤 {display_name(user)}",
        f"ID: {user['_id']}",
        f"Balance: {format_amount(float(user['balance']))} TON",
        f"Total wagered: {format_amount(float(user['total_wagered']))} TON",
        f"Wins: {user['total_wins']} | Losses: {user['total_losses']}",
        f"Profit/Loss: {format_amount(float(user['total_profit']))} TON",
        f"Games played: {user['games_played']}",
        f"VIP: {'Yes 👑' if user.get('is_vip') else 'No'}",
        f"MLBB ID: {user.get('mlbb_id') or 'Not set'}",
    ]
    if settings.sandbox_mode:
        lines.append(sandbox_note())
    await update.effective_message.reply_text("\n".join(lines))


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
        "Last 10 transactions\n"
        + "\n".join(tx_lines)
        + "\n\nLast 10 matches\n"
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
        await update.effective_message.reply_text("Usage: /setmlbb <mlbb_id>")
        return
    from db.mongo import get_db

    db = await get_db()
    await db.users.update_one(
        {"_id": str(user["_id"])},
        {"$set": {"mlbb_id": context.args[0].strip(), "mlbb_verified": False, "last_active": utcnow()}},
    )
    await update.effective_message.reply_text("✅ MLBB ID saved.")


@guard_handler
async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    action = query.data

    if action == "menu:balance":
        await balance_command(update, context)
    elif action == "menu:games":
        await query.message.reply_text(
            "🎮 Choose a game and create a challenge:\n\n"
            "/challenge <amount> dice [normal|crazy] [1|2|3]\n"
            "/challenge <amount> football [normal|crazy] [1|2|3]\n"
            "/challenge <amount> chess\n"
            "/challenge <amount> mlbb",
            reply_markup=games_keyboard(),
        )
    elif action == "menu:deposit":
        await deposit_command(update, context)
    elif action == "menu:withdraw":
        await query.message.reply_text("Use /withdraw <amount> <address>")
    elif action == "menu:profile":
        await profile_command(update, context)
    elif action == "menu:history":
        await history_command(update, context)
    elif action == "menu:leaderboard":
        await leaderboard_command(update, context)
    elif action == "menu:tip":
        await query.message.reply_text("Use /tip <@username|user_id> <amount>")
    elif action == "menu:admin":
        if not is_private_chat(update):
            await query.message.reply_text("Admin panel is available in private chat only.")
            return
        if update.effective_user.id not in settings.admin_ids:
            await query.message.reply_text("Unauthorized.")
            return
        await query.message.reply_text(
            "\n".join(
                [
                    "Admin commands:",
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
            hint = f"/challenge <amount> {selected} [normal|crazy] [1|2|3]"
        elif selected == "chess":
            hint = "/challenge <amount> chess"
        elif selected == "mlbb":
            hint = "/challenge <amount> mlbb"
        else:
            hint = f"/challenge <amount> {selected}"
        await query.message.reply_text(f"🎮 {selected.title()} — Create a challenge:\n{hint}")


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
                    f"Possible manual deposit evidence from {display_name(user)} ({user['_id']})\n"
                    f"Message: {text}\n"
                    f"Approve with /approve_deposit {user['_id']} <amount> <crypto>"
                ),
            )
        except Exception:
            pass
    await update.effective_message.reply_text(
        "Your message was forwarded to admins for manual deposit review."
    )
