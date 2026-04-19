from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from bot.games import cancel_match_and_refund, settle_match
from bot.payments import approve_withdrawal_record, reject_withdrawal_record
from config import settings
from db.models import (
    add_balance,
    add_transaction,
    admin_stats,
    get_match,
    get_pending_withdrawal,
    get_user,
    list_active_matches,
    list_transactions_for_user,
    list_matches_for_user,
    set_settings_values,
    sync_vip_status_all,
    top_wagerers,
    update_match,
    update_pending_withdrawal,
)
from db.mongo import get_db
from utils import display_name, format_amount, utcnow, win_rate


def is_admin(user_id: int) -> bool:
    return user_id in settings.admin_ids


async def require_admin(update: Update) -> bool:
    if update.effective_chat.type != "private":
        await update.effective_message.reply_text("Admin commands only work in private chat.")
        return False
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("Unauthorized.")
        return False
    return True


async def add_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /add_balance <user_id> <amount> [reason]")
        return
    user_id = context.args[0]
    amount = float(context.args[1])
    reason = " ".join(context.args[2:]).strip() or None
    user = await add_balance(user_id, amount, reason=reason, admin_id=update.effective_user.id)
    if not user:
        await update.effective_message.reply_text("User not found.")
        return
    await update.effective_message.reply_text(f"Added {format_amount(amount)} TON to {display_name(user)}.")


async def deduct_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /deduct_balance <user_id> <amount> [reason]")
        return
    user_id = context.args[0]
    amount = float(context.args[1])
    from db.models import reserve_balance

    if not await reserve_balance(user_id, amount):
        await update.effective_message.reply_text("User not found or insufficient balance.")
        return
    await add_transaction(user_id, "house", -amount, "completed", admin_id=update.effective_user.id, metadata={"reason": " ".join(context.args[2:]).strip() or None})
    await update.effective_message.reply_text(f"Deducted {format_amount(amount)} TON from {user_id}.")


async def approve_withdrawal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /approve_withdrawal <withdrawal_id>")
        return
    withdrawal = await get_pending_withdrawal(context.args[0])
    if not withdrawal or withdrawal["status"] != "pending":
        await update.effective_message.reply_text("Withdrawal not found or already resolved.")
        return
    await update_pending_withdrawal(
        context.args[0],
        {"status": "approved", "resolved_at": utcnow(), "admin_id": str(update.effective_user.id)},
    )
    await approve_withdrawal_record(withdrawal, update.effective_user.id)
    await update.effective_message.reply_text(f"Approved withdrawal `{context.args[0]}`.")
    await context.bot.send_message(
        chat_id=int(withdrawal["user_id"]),
        text=f"Your withdrawal `{context.args[0]}` was approved. Net amount: {format_amount(float(withdrawal['net_amount']))} TON.",
    )


async def reject_withdrawal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /reject_withdrawal <withdrawal_id> [reason]")
        return
    reason = " ".join(context.args[1:]).strip() or None
    withdrawal = await get_pending_withdrawal(context.args[0])
    if not withdrawal or withdrawal["status"] != "pending":
        await update.effective_message.reply_text("Withdrawal not found or already resolved.")
        return
    await update_pending_withdrawal(
        context.args[0],
        {"status": "rejected", "resolved_at": utcnow(), "admin_id": str(update.effective_user.id)},
    )
    await reject_withdrawal_record(withdrawal, update.effective_user.id, reason)
    await update.effective_message.reply_text(f"Rejected withdrawal `{context.args[0]}`.")
    await context.bot.send_message(
        chat_id=int(withdrawal["user_id"]),
        text=f"Your withdrawal `{context.args[0]}` was rejected.{f' Reason: {reason}' if reason else ''}",
    )


async def approve_deposit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 3:
        await update.effective_message.reply_text("Usage: /approve_deposit <user_id> <amount> <crypto>")
        return
    user_id, amount_raw, crypto = context.args
    amount = float(amount_raw)
    db = await get_db()
    await db.users.update_one({"_id": user_id}, {"$inc": {"balance": amount}})
    user = await get_user(user_id)
    if not user:
        await update.effective_message.reply_text("User not found.")
        return
    from services.house import add_house_deposit

    await add_house_deposit(amount)
    await add_transaction(user_id, "deposit", amount, "completed", crypto=crypto, admin_id=update.effective_user.id)
    await update.effective_message.reply_text(f"Credited {format_amount(amount)} {crypto} to {display_name(user)}.")
    await context.bot.send_message(chat_id=int(user_id), text=f"Your {crypto} deposit of {format_amount(amount)} has been approved.")


async def resolve_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 2:
        await update.effective_message.reply_text("Usage: /resolve <match_id> <winner_user_id>")
        return
    match = await get_match(context.args[0])
    if not match or match["status"] not in {"active", "disputed"}:
        await update.effective_message.reply_text("Match not found or not resolvable.")
        return
    winner_id = context.args[1]
    if winner_id not in {str(match["challenger_id"]), str(match["opponent_id"])}:
        await update.effective_message.reply_text("Winner must be one of the match players.")
        return
    settled, payout, _ = await settle_match(match, winner_id)
    await update.effective_message.reply_text(f"Resolved match `{settled['_id']}`. Winner payout: {format_amount(payout)} TON.")


async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    stats = await admin_stats()
    house = stats["house"]
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"Total users: {stats['total_users']}",
                f"Active matches: {stats['active_matches']}",
                f"Pending matches: {stats['pending_matches']}",
                f"Disputed matches: {stats['disputed_matches']}",
                f"House balance: {format_amount(float(house['balance']))} TON",
                f"Total deposited: {format_amount(float(house['total_deposited']))} TON",
                f"Total withdrawn: {format_amount(float(house['total_withdrawn']))} TON",
                f"Fees collected: {format_amount(float(house['total_fees_collected']))} TON",
            ]
        )
    )


async def wager_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    users = await top_wagerers(limit=10)
    lines = ["Top 10 wagerers"]
    for idx, user in enumerate(users, start=1):
        rate = win_rate(int(user["total_wins"]), int(user["total_losses"]))
        lines.append(
            f"{idx}. {display_name(user)} — wagered {format_amount(float(user['total_wagered']))} TON | games {user['games_played']} | win rate {rate}%"
        )
    await update.effective_message.reply_text("\n".join(lines))


async def admin_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /admin_user <user_id>")
        return
    user = await get_user(context.args[0])
    if not user:
        await update.effective_message.reply_text("User not found.")
        return
    transactions = await list_transactions_for_user(user["_id"], limit=10)
    matches = await list_matches_for_user(user["_id"], limit=10)
    tx_lines = [f"{tx['type']} {format_amount(float(tx['amount']))} {tx['status']}" for tx in transactions] or ["No transactions."]
    match_lines = [f"{match['_id']} {match['game']} {match['status']}" for match in matches] or ["No matches."]
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"User: {display_name(user)}",
                f"ID: {user['_id']}",
                f"Balance: {format_amount(float(user['balance']))} TON",
                f"Banned: {user.get('is_banned')}",
                f"VIP: {user.get('is_vip')}",
                f"MLBB ID: {user.get('mlbb_id') or 'Not set'}",
                "Transactions:",
                *tx_lines,
                "Matches:",
                *match_lines,
            ]
        )
    )


async def admin_matches_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    matches = await list_active_matches(limit=20)
    if not matches:
        await update.effective_message.reply_text("No active or pending matches.")
        return
    lines = [
        f"{match['_id']} {match['game']} {match['status']} {format_amount(float(match['amount']))} TON"
        for match in matches
    ]
    await update.effective_message.reply_text("\n".join(lines))


async def admin_ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /admin_ban <user_id>")
        return
    db = await get_db()
    result = await db.users.update_one({"_id": context.args[0]}, {"$set": {"is_banned": True}})
    if result.modified_count != 1:
        await update.effective_message.reply_text("User not found.")
        return
    await update.effective_message.reply_text(f"Banned {context.args[0]}.")


async def admin_unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /admin_unban <user_id>")
        return
    db = await get_db()
    result = await db.users.update_one({"_id": context.args[0]}, {"$set": {"is_banned": False}})
    if result.modified_count != 1:
        await update.effective_message.reply_text("User not found.")
        return
    await update.effective_message.reply_text(f"Unbanned {context.args[0]}.")


async def set_fee_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /set_fee <percentage>")
        return
    percentage = float(context.args[0])
    await set_settings_values({"withdrawal_fee_percent": percentage})
    await update.effective_message.reply_text(f"Withdrawal fee set to {percentage}%.")


async def set_min_wager_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /set_min_wager <amount>")
        return
    amount = float(context.args[0])
    await set_settings_values({"min_wager_threshold": amount})
    await sync_vip_status_all()
    await update.effective_message.reply_text(f"VIP threshold updated to {format_amount(amount)} TON.")


async def set_deposit_address_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 2:
        await update.effective_message.reply_text("Usage: /set_deposit_address <TON|USDT_BEP20|SOL> <address>")
        return
    crypto = context.args[0].upper()
    if crypto not in {"TON", "USDT_BEP20", "SOL"}:
        await update.effective_message.reply_text("Crypto must be TON, USDT_BEP20, or SOL.")
        return
    db = await get_db()
    await db.settings.update_one(
        {"_id": "singleton"},
        {"$set": {f"deposit_addresses.{crypto}": context.args[1], "updated_at": utcnow()}},
        upsert=True,
    )
    await update.effective_message.reply_text(f"Updated {crypto} deposit address.")


async def admin_refund_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /admin_refund <match_id>")
        return
    match = await get_match(context.args[0])
    if not match or match["status"] == "completed":
        await update.effective_message.reply_text("Match not found or already completed.")
        return
    await cancel_match_and_refund(match)
    await update.effective_message.reply_text(f"Refunded match `{context.args[0]}`.")


async def admin_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    stats = await admin_stats()
    house = stats["house"]
    await update.effective_message.reply_text(
        "\n".join(
            [
                f"House balance: {format_amount(float(house['balance']))} TON",
                f"Fees collected: {format_amount(float(house['total_fees_collected']))} TON",
                f"Total deposited: {format_amount(float(house['total_deposited']))} TON",
                f"Total withdrawn: {format_amount(float(house['total_withdrawn']))} TON",
            ]
        )
    )
