from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from bot.games import (
    GAME_FEE_RATE,
    accept_challenge_and_activate,
    cancel_match_and_refund,
    challenge_summary,
    create_challenge,
    settle_match,
)
from bot.keyboards import accept_challenge_keyboard, deposit_keyboard, games_keyboard, main_menu_keyboard
from bot.payments import create_withdrawal_request, describe_balance, notify_deposit_prompt, tip_users
from config import settings
from db.models import (
    add_balance,
    add_transaction,
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
    is_group_chat,
    is_private_chat,
    parse_user_reference,
    private_only_markup,
    win_rate,
)


async def ensure_current_user(update: Update) -> dict[str, Any]:
    from db.models import ensure_user

    user = await ensure_user(
        update.effective_user.id,
        update.effective_user.username,
        update.effective_user.first_name,
    )
    if user.get("is_banned"):
        raise PermissionError("You are banned from using this bot.")
    return user


async def reject_private_only(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if is_private_chat(update):
        return False
    await update.effective_message.reply_text(
        PRIVATE_ONLY_TEXT.format(link=bot_private_link(context.bot.username)),
        reply_markup=private_only_markup(context.bot.username),
    )
    return True


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"Welcome, {display_name(user)}",
                "Use the menu below to manage your wallet, play games, and track your stats.",
            ]
        ),
        reply_markup=main_menu_keyboard(),
    )


async def deposit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await ensure_current_user(update)
    if await reject_private_only(update, context):
        return
    await update.effective_message.reply_text(
        "Choose a deposit method. Use your Telegram user ID as the memo/comment for incoming funds.",
        reply_markup=deposit_keyboard(),
    )


async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if await reject_private_only(update, context):
        return
    if len(context.args) != 2:
        await update.effective_message.reply_text("Usage: /withdraw <amount> <ton_address>")
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
        await update.effective_message.reply_text(
            f"Insufficient balance. You need {format_amount(total_cost)} TON including fee."
        )
        return
    from db.models import reserve_balance

    if not await reserve_balance(user["_id"], total_cost):
        await update.effective_message.reply_text("Balance changed before withdrawal request could be reserved. Try again.")
        return
    withdrawal_id, _, net_amount = await create_withdrawal_request(user_id=update.effective_user.id, amount=amount, address=context.args[1].strip())
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"Withdrawal queued with ID `{withdrawal_id}`",
                f"Gross amount: {format_amount(amount)} TON",
                f"Net after fee: {format_amount(net_amount)} TON",
                "Admins have been notified for approval.",
            ]
        )
    )
    for admin_id in settings.admin_ids:
        await context.bot.send_message(
            chat_id=admin_id,
            text=(
                f"New withdrawal request\n"
                f"ID: `{withdrawal_id}`\n"
                f"User: {display_name(user)} ({user['_id']})\n"
                f"Amount: {format_amount(amount)} TON\n"
                f"Address: {context.args[1].strip()}\n"
                f"Approve with /approve_withdrawal {withdrawal_id}"
            ),
        )


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if await reject_private_only(update, context):
        return
    await update.effective_message.reply_text(f"Balance: {format_amount(float(user['balance']))} TON")


async def tip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if len(context.args) != 2:
        await update.effective_message.reply_text("Usage: /tip <@username or user_id> <amount>")
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
    if not recipient:
        await update.effective_message.reply_text("Recipient is not registered yet.")
        return
    if str(recipient["_id"]) == str(user["_id"]):
        await update.effective_message.reply_text("You cannot tip yourself.")
        return
    from db.models import reserve_balance, refund_balance

    if not await reserve_balance(user["_id"], amount):
        await update.effective_message.reply_text("Insufficient balance.")
        return
    await refund_balance(recipient["_id"], amount)
    await tip_users(update.effective_user.id, recipient["_id"], amount)
    await update.effective_message.reply_text(
        f"Sent {format_amount(amount)} TON to {display_name(recipient)}."
    )
    await context.bot.send_message(
        chat_id=int(recipient["_id"]),
        text=f"You received a tip of {format_amount(amount)} TON from {display_name(user)}.",
    )


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
    if game not in {"dice", "chess", "mlbb", "football"}:
        await update.effective_message.reply_text("Game must be one of: dice, chess, mlbb, football")
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
    try:
        match = await create_challenge(user, amount, game, mode, dice_count, update.effective_chat.id)
    except ValueError as exc:
        await update.effective_message.reply_text(str(exc))
        return
    summary = challenge_summary(match, user)
    await update.effective_message.reply_text(summary, reply_markup=accept_challenge_keyboard(match["_id"]))


async def accept_command(update: Update, context: ContextTypes.DEFAULT_TYPE, match_id: str | None = None) -> None:
    user = await ensure_current_user(update)
    target_match_id = match_id or (context.args[0] if context.args else "")
    if not target_match_id:
        await update.effective_message.reply_text("Usage: /accept <match_id>")
        return
    match = await get_match(target_match_id)
    if not match or match["status"] != "pending":
        await update.effective_message.reply_text("This match is not available.")
        return
    if str(match["challenger_id"]) == str(user["_id"]):
        await update.effective_message.reply_text("You cannot accept your own challenge.")
        return
    if match["game"] == "mlbb" and not user.get("mlbb_id"):
        await update.effective_message.reply_text("Set your MLBB ID first with /setmlbb <mlbb_id>.")
        return
    try:
        active_match = await accept_challenge_and_activate(match, user)
    except ValueError as exc:
        await update.effective_message.reply_text(str(exc))
        return
    challenger = await get_user(active_match["challenger_id"])
    opponent = user
    amount = float(active_match["amount"])
    if active_match["game"] in {"dice", "football"}:
        emoji = "🎲" if active_match["game"] == "dice" else "⚽"
        challenger_total = 0
        opponent_total = 0
        mode = active_match.get("mode", "normal")
        dice_count = int(active_match.get("dice_count", 1))
        while True:
            challenger_total = 0
            opponent_total = 0
            for _ in range(dice_count):
                first_roll = await context.bot.send_dice(chat_id=update.effective_chat.id, emoji=emoji)
                second_roll = await context.bot.send_dice(chat_id=update.effective_chat.id, emoji=emoji)
                challenger_total += int(first_roll.dice.value)
                opponent_total += int(second_roll.dice.value)
            if challenger_total != opponent_total:
                break
            await update.effective_message.reply_text("Tie detected, re-rolling automatically.")
        if mode == "crazy":
            winner_id = active_match["challenger_id"] if challenger_total < opponent_total else active_match["opponent_id"]
        else:
            winner_id = active_match["challenger_id"] if challenger_total > opponent_total else active_match["opponent_id"]
        settled, payout, fee = await settle_match(active_match, winner_id)
        winner = challenger if str(winner_id) == str(challenger["_id"]) else opponent
        await update.effective_message.reply_text(
            "\n".join(
                [
                    f"{active_match['game'].title()} result for match `{settled['_id']}`",
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
        group_text = "\n".join(
            [
                f"Chess match `{active_match['_id']}` is live.",
                f"{display_name(challenger)} vs {display_name(opponent)}",
                f"Wager: {format_amount(amount)} TON each",
                "Both players can open their match links below.",
                ANTI_CHEAT_WARNING,
            ]
        )
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Open Challenger Board", url=challenger_url)],
                [InlineKeyboardButton("Open Opponent Board", url=opponent_url)],
            ]
        )
        await update.effective_message.reply_text(group_text, reply_markup=keyboard)
        return
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"MLBB match `{active_match['_id']}` is active.",
                f"{display_name(challenger)} vs {display_name(opponent)}",
                f"Wager: {format_amount(amount)} TON each",
                f"{display_name(challenger)} MLBB ID: {challenger.get('mlbb_id')}",
                f"{display_name(opponent)} MLBB ID: {opponent.get('mlbb_id')}",
                f"Report result with /result {active_match['_id']} win or /result {active_match['_id']} lose",
                ANTI_CHEAT_WARNING,
            ]
        )
    )


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
        await update.effective_message.reply_text("No active MLBB match found with that ID.")
        return
    if str(match["challenger_id"]) == str(user["_id"]):
        match = await update_match(match_id, {"challenger_result": result})
    else:
        match = await update_match(match_id, {"opponent_result": result})
    if not match:
        await update.effective_message.reply_text("Failed to update result.")
        return
    challenger_result = match.get("challenger_result")
    opponent_result = match.get("opponent_result")
    if not challenger_result or not opponent_result:
        await update.effective_message.reply_text("Result recorded. Waiting for the other player.")
        return
    if challenger_result == opponent_result:
        await update_match(match_id, {"status": "disputed"})
        for admin_id in settings.admin_ids:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"Disputed MLBB match `{match_id}`. Resolve with /resolve {match_id} <winner_user_id>",
            )
        await update.effective_message.reply_text(
            f"Match `{match_id}` is now disputed. Admins have been notified.\n{ANTI_CHEAT_WARNING}"
        )
        return
    winner_id = match["challenger_id"] if challenger_result == "win" else match["opponent_id"]
    settled, payout, fee = await settle_match(match, winner_id)
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


async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if await reject_private_only(update, context):
        return
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"Profile for {display_name(user)}",
                f"User ID: {user['_id']}",
                f"Balance: {format_amount(float(user['balance']))} TON",
                f"Total Wagered: {format_amount(float(user['total_wagered']))} TON",
                f"Wins: {user['total_wins']} | Losses: {user['total_losses']}",
                f"Profit/Loss: {format_amount(float(user['total_profit']))} TON",
                f"Games Played: {user['games_played']}",
                f"VIP: {'Yes 👑' if user.get('is_vip') else 'No'}",
                f"MLBB ID: {user.get('mlbb_id') or 'Not set'}",
            ]
        )
    )


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if await reject_private_only(update, context):
        return
    transactions = await list_transactions_for_user(user["_id"], limit=10)
    matches = await list_matches_for_user(user["_id"], limit=10)
    tx_lines = [
        f"{tx['type']} {format_amount(float(tx['amount']))} TON [{tx['status']}]"
        for tx in transactions
    ] or ["No transactions yet."]
    match_lines = [
        f"{match['_id']} {match['game']} {format_amount(float(match['amount']))} TON [{match['status']}] winner={match.get('winner_id') or '-'}"
        for match in matches
    ] or ["No matches yet."]
    await update.effective_message.reply_text(
        "Last 10 transactions\n"
        + "\n".join(tx_lines)
        + "\n\nLast 10 matches\n"
        + "\n".join(match_lines)
    )


async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await ensure_current_user(update)
    rows = await top_wagerers(limit=10)
    if not rows:
        await update.effective_message.reply_text("No leaderboard data yet.")
        return
    lines = ["Top Wagerers"]
    for idx, row in enumerate(rows, start=1):
        badge = " 👑" if row.get("is_vip") else ""
        lines.append(f"{idx}. {display_name(row)}{badge} — {format_amount(float(row['total_wagered']))} TON")
    await update.effective_message.reply_text("\n".join(lines))


async def setmlbb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if await reject_private_only(update, context):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /setmlbb <mlbb_id>")
        return
    from db.mongo import get_db

    db = await get_db()
    await db.users.update_one(
        {"_id": str(user["_id"])},
        {"$set": {"mlbb_id": context.args[0].strip(), "mlbb_verified": False}},
    )
    await update.effective_message.reply_text("MLBB ID saved.")


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
        await query.message.reply_text("Use /withdraw <amount> <ton_address>")
    elif action == "menu:profile":
        await profile_command(update, context)
    elif action == "menu:history":
        await history_command(update, context)
    elif action == "menu:leaderboard":
        await leaderboard_command(update, context)
    elif action == "menu:tip":
        await query.message.reply_text("Use /tip <@username or user_id> <amount>")
    elif action.startswith("deposit:"):
        crypto = action.split(":", 1)[1]
        await notify_deposit_prompt(update, context, crypto)
    elif action.startswith("games:"):
        game = action.split(":", 1)[1]
        await query.message.reply_text(f"Use /challenge <amount> {game} [mode] [dice_count]")


async def accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    match_id = query.data.split(":", 1)[1]
    await accept_command(update, context, match_id=match_id)


async def fallback_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_current_user(update)
    if not is_private_chat(update):
        return
    text = (update.effective_message.text or "").strip()
    if len(text) < 10 or text.startswith("/"):
        return
    for admin_id in settings.admin_ids:
        await context.bot.send_message(
            chat_id=admin_id,
            text=(
                f"Possible manual deposit evidence from {display_name(user)} ({user['_id']})\n"
                f"Message: {text}\n"
                f"Approve with /approve_deposit {user['_id']} <amount> <crypto>"
            ),
        )
    await update.effective_message.reply_text("Your message has been forwarded to admins for manual deposit review.")
