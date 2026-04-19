from __future__ import annotations

from functools import wraps
from typing import Any, Awaitable, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from bot.games import challenge_summary, claim_and_activate_match, create_challenge, roll_competitive_dice, settle_match
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
    utcnow,
)

UserHandler = Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]


def guard_handler(func: UserHandler) -> UserHandler:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
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
                f"Welcome, {display_name(user)}",
                "Use the menu below to access balance, games, deposits, withdrawals, history, and leaderboard.",
            ]
        ),
        reply_markup=main_menu_keyboard(),
    )


@guard_handler
async def deposit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await ensure_current_user(update)
    if not await require_private(update, context):
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
    from db.models import reserve_balance
    from bot.keyboards import withdrawal_admin_keyboard

    if not await reserve_balance(user["_id"], total_cost):
        await update.effective_message.reply_text("Balance changed before reservation. Try again.")
        return
    withdrawal_id, _, net_amount = await create_withdrawal_request(update.effective_user.id, amount, context.args[1].strip())
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
    await update.effective_message.reply_text(f"💰 Balance: {format_amount(float(user['balance']))} TON")


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
    if not await transfer_tip(user["_id"], recipient["_id"], amount):
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
    user = await ensure_current_user(update)
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /challenge <amount> <game> [mode] [dice_count]")
        return
    try:
        amount = round(float(context.args[0]), 8)
    except ValueError:
        await update.effective_message.reply_text("Amount must be numeric.")
        return
    game = context.args[1].strip().lower()
    if game not in {"dice", "football", "chess", "mlbb"}:
        await update.effective_message.reply_text("Game must be one of: dice, football, chess, mlbb")
        return
    mode = context.args[2].strip().lower() if len(context.args) >= 3 else "normal"
    dice_count = int(context.args[3]) if len(context.args) >= 4 and context.args[3].isdigit() else 1
    if game in {"dice", "football"}:
        if mode not in {"normal", "crazy"}:
            await update.effective_message.reply_text("Mode must be normal or crazy.")
            return
        if dice_count not in {1, 2, 3}:
            await update.effective_message.reply_text("Dice count must be 1, 2, or 3.")
            return
    else:
        mode = "normal"
        dice_count = 1
    if game == "mlbb" and not user.get("mlbb_id"):
        await update.effective_message.reply_text("Set your MLBB ID first with /setmlbb <mlbb_id>.")
        return
    match = await create_challenge(user, amount, game, mode, dice_count, update.effective_chat.id)
    await update.effective_message.reply_text(
        challenge_summary(match, user),
        reply_markup=accept_challenge_keyboard(match["_id"]),
    )


@guard_handler
async def accept_command(update: Update, context: ContextTypes.DEFAULT_TYPE, match_id: str | None = None) -> None:
    user = await ensure_current_user(update)
    target_match_id = match_id or (context.args[0] if context.args else "")
    if not target_match_id:
        await update.effective_message.reply_text("Usage: /accept <match_id>")
        return
    match = await get_match(target_match_id)
    if not match or match["status"] != "pending":
        await update.effective_message.reply_text("Match already taken or unavailable.")
        return
    if str(match["challenger_id"]) == str(user["_id"]):
        await update.effective_message.reply_text("You cannot accept your own challenge.")
        return
    if match["game"] == "mlbb" and not user.get("mlbb_id"):
        await update.effective_message.reply_text("Set your MLBB ID first with /setmlbb <mlbb_id>.")
        return
    active_match = await claim_and_activate_match(match, user)
    challenger = await get_user(active_match["challenger_id"])
    opponent = user
    amount = float(active_match["amount"])
    if active_match["game"] in {"dice", "football"}:
        emoji = "🎲" if active_match["game"] == "dice" else "⚽"
        challenger_total, opponent_total = await roll_competitive_dice(update, context, emoji, int(active_match.get("dice_count", 1)))
        if active_match.get("mode") == "crazy":
            winner_id = active_match["challenger_id"] if challenger_total < opponent_total else active_match["opponent_id"]
        else:
            winner_id = active_match["challenger_id"] if challenger_total > opponent_total else active_match["opponent_id"]
        settled, payout, fee = await settle_match(active_match, str(winner_id))
        winner = challenger if str(winner_id) == str(challenger["_id"]) else opponent
        title = "Football" if active_match["game"] == "football" else "Dice"
        await update.effective_message.reply_text(
            "\n".join(
                [
                    f"{title} result for match `{settled['_id']}`",
                    f"{display_name(challenger)}: {challenger_total}",
                    f"{display_name(opponent)}: {opponent_total}",
                    f"Winner: {display_name(winner)}",
                    f"Payout: {format_amount(payout)} TON",
                    f"House fee: {format_amount(fee)} TON",
                    ANTI_CHEAT_WARNING,
                ]
            )
        )
        return
    if active_match["game"] == "chess":
        base_url = settings.app_base_url or settings.webhook_url
        challenger_url = f"{base_url}/chess?match_id={active_match['_id']}&user_id={challenger['_id']}"
        opponent_url = f"{base_url}/chess?match_id={active_match['_id']}&user_id={opponent['_id']}"
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Open Challenger Board", url=challenger_url)],
                [InlineKeyboardButton("Open Opponent Board", url=opponent_url)],
            ]
        )
        await update.effective_message.reply_text(
            "\n".join(
                [
                    f"♟️ Chess match `{active_match['_id']}` is live.",
                    f"{display_name(challenger)} vs {display_name(opponent)}",
                    f"Wager: {format_amount(amount)} TON each",
                    ANTI_CHEAT_WARNING,
                ]
            ),
            reply_markup=keyboard,
        )
        return
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"🎮 MLBB match `{active_match['_id']}` is active.",
                f"{display_name(challenger)} vs {display_name(opponent)}",
                f"{display_name(challenger)} MLBB ID: {challenger.get('mlbb_id')}",
                f"{display_name(opponent)} MLBB ID: {opponent.get('mlbb_id')}",
                f"Report with /result {active_match['_id']} win or /result {active_match['_id']} lose",
                ANTI_CHEAT_WARNING,
            ]
        )
    )


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
    values = {"challenger_result": result} if str(match["challenger_id"]) == str(user["_id"]) else {"opponent_result": result}
    match = await update_match(match_id, values)
    if not match:
        await update.effective_message.reply_text("Failed to update result.")
        return
    if not match.get("challenger_result") or not match.get("opponent_result"):
        await update.effective_message.reply_text("Result recorded. Waiting for the other player.")
        return
    if match["challenger_result"] == match["opponent_result"]:
        await update_match(match_id, {"status": "disputed"})
        for admin_id in settings.admin_ids:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"Disputed MLBB match `{match_id}`. Resolve with /resolve {match_id} <winner_user_id>",
                )
            except Exception:
                pass
        await update.effective_message.reply_text(f"⚠️ Match `{match_id}` is disputed.\n{ANTI_CHEAT_WARNING}")
        return
    winner_id = match["challenger_id"] if match["challenger_result"] == "win" else match["opponent_id"]
    settled, payout, fee = await settle_match(match, str(winner_id))
    winner = await get_user(winner_id)
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"MLBB match `{settled['_id']}` resolved.",
                f"Winner: {display_name(winner)}",
                f"Payout: {format_amount(payout)} TON",
                f"House fee: {format_amount(fee)} TON",
                ANTI_CHEAT_WARNING,
            ]
        )
    )


@guard_handler
async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    await update.effective_message.reply_text(
        "\n".join(
            [
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
        )
    )


@guard_handler
async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if not await require_private(update, context):
        return
    transactions = await list_transactions_for_user(user["_id"], limit=10)
    matches = await list_matches_for_user(user["_id"], limit=10)
    tx_lines = [f"{tx['type']} {format_amount(float(tx['amount']))} TON [{tx['status']}]" for tx in transactions] or ["No transactions yet."]
    match_lines = [f"{match['_id']} {match['game']} {format_amount(float(match['amount']))} TON [{match['status']}]" for match in matches] or ["No matches yet."]
    await update.effective_message.reply_text(
        "Last 10 transactions\n"
        + "\n".join(tx_lines)
        + "\n\nLast 10 matches\n"
        + "\n".join(match_lines)
    )


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
    await update.effective_message.reply_text("MLBB ID saved.")


@guard_handler
async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    action = query.data
    if action == "menu:balance":
        await balance_command(update, context)
    elif action == "menu:games":
        await query.message.reply_text("Available games", reply_markup=games_keyboard())
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
    elif action.startswith("deposit:"):
        await notify_deposit_prompt(update, context, action.split(":", 1)[1])
    elif action.startswith("games:"):
        await query.message.reply_text(f"Use /challenge <amount> {action.split(':', 1)[1]} [mode] [dice_count]")


@guard_handler
async def accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    await accept_command(update, context, match_id=query.data.split(":", 1)[1])


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
    await update.effective_message.reply_text("Your message was forwarded to admins for manual deposit review.")
