from __future__ import annotations

from functools import wraps
from typing import Awaitable, Callable

from telegram import Update
from telegram.ext import ContextTypes

from bot.games import cancel_match_and_refund, settle_match
from bot.payments import approve_withdrawal_record, reject_withdrawal_record
from config import logger, settings
from db.models import (
    add_balance,
    add_transaction,
    admin_force_deduct_balance,
    admin_stats,
    cancel_pending_matches_for_user,
    get_match,
    get_pending_withdrawal,
    get_settings_doc,
    get_user,
    list_active_matches,
    list_matches_for_user,
    list_transactions_for_user,
    set_settings_values,
    sync_vip_status_all,
    top_wagerers,
    update_pending_withdrawal,
)
from utils import display_name, format_amount, utcnow, win_rate

AdminHandler = Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]


def guard_admin(func: AdminHandler) -> AdminHandler:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            await func(update, context)
        except Exception as exc:
            logger.exception("Admin handler %s failed: %s", func.__name__, exc)
            if update.effective_message:
                await update.effective_message.reply_text("⚠️ Admin action failed.")

    return wrapper  # type: ignore[return-value]


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


async def require_finance_enabled(update: Update) -> bool:
    if settings.sandbox_mode:
        await update.effective_message.reply_text("This financial admin command is disabled in sandbox mode.")
        return False
    return True


@guard_admin
async def add_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if not await require_finance_enabled(update):
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /add_balance <user_id> <amount> [reason]")
        return
    user_id = context.args[0]
    amount = float(context.args[1])
    reason = " ".join(context.args[2:]).strip() or None
    user = await add_balance(user_id, amount, reason=reason, tx_type="admin_credit", admin_id=update.effective_user.id)
    if not user:
        await update.effective_message.reply_text("User not found.")
        return
    await update.effective_message.reply_text(f"Added {format_amount(amount)} TON to {display_name(user)}.")
    try:
        await context.bot.send_message(chat_id=int(user_id), text=f"Admin credited {format_amount(amount)} TON to your balance.")
    except Exception:
        pass


@guard_admin
async def deduct_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if not await require_finance_enabled(update):
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /deduct_balance <user_id> <amount> [reason]")
        return
    user_id = context.args[0]
    amount = float(context.args[1])
    reason = " ".join(context.args[2:]).strip() or None
    user = await admin_force_deduct_balance(user_id, amount)
    if not user:
        await update.effective_message.reply_text("User not found.")
        return
    await add_transaction(user_id, "admin_deduct", -amount, "completed", admin_id=update.effective_user.id, metadata={"reason": reason})
    await update.effective_message.reply_text(f"Deducted {format_amount(amount)} TON from {display_name(user)}.")
    try:
        await context.bot.send_message(chat_id=int(user_id), text=f"Admin deducted {format_amount(amount)} TON from your balance.")
    except Exception:
        pass


@guard_admin
async def approve_withdrawal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if not await require_finance_enabled(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /approve_withdrawal <withdrawal_id>")
        return
    withdrawal = await get_pending_withdrawal(context.args[0])
    if not withdrawal or withdrawal["status"] != "pending":
        await update.effective_message.reply_text("Withdrawal not found or already resolved.")
        return
    await update_pending_withdrawal(context.args[0], {"status": "approved", "resolved_at": utcnow(), "admin_id": str(update.effective_user.id)})
    await approve_withdrawal_record(withdrawal, update.effective_user.id)
    await update.effective_message.reply_text(f"Approved withdrawal `{context.args[0]}`.")
    try:
        await context.bot.send_message(
            chat_id=int(withdrawal["user_id"]),
            text=f"Your withdrawal `{context.args[0]}` was approved. Net amount: {format_amount(float(withdrawal['net_amount']))} TON.",
        )
    except Exception:
        pass


@guard_admin
async def reject_withdrawal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if not await require_finance_enabled(update):
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /reject_withdrawal <withdrawal_id> [reason]")
        return
    reason = " ".join(context.args[1:]).strip() or None
    withdrawal = await get_pending_withdrawal(context.args[0])
    if not withdrawal or withdrawal["status"] != "pending":
        await update.effective_message.reply_text("Withdrawal not found or already resolved.")
        return
    await update_pending_withdrawal(context.args[0], {"status": "rejected", "resolved_at": utcnow(), "admin_id": str(update.effective_user.id)})
    await reject_withdrawal_record(withdrawal, update.effective_user.id, reason)
    await update.effective_message.reply_text(f"Rejected withdrawal `{context.args[0]}`.")
    try:
        await context.bot.send_message(
            chat_id=int(withdrawal["user_id"]),
            text=f"Your withdrawal `{context.args[0]}` was rejected.{f' Reason: {reason}' if reason else ''}",
        )
    except Exception:
        pass


@guard_admin
async def approve_deposit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if not await require_finance_enabled(update):
        return
    if len(context.args) != 3:
        await update.effective_message.reply_text("Usage: /approve_deposit <user_id> <amount> <crypto>")
        return
    user_id, amount_raw, crypto = context.args
    amount = float(amount_raw)
    user = await add_balance(user_id, amount, reason=f"manual_{crypto.lower()}_deposit", tx_type="deposit", admin_id=update.effective_user.id)
    if not user:
        await update.effective_message.reply_text("User not found.")
        return
    from services.house import add_house_deposit

    await add_house_deposit(amount)
    await update.effective_message.reply_text(f"Credited {format_amount(amount)} {crypto} to {display_name(user)}.")
    try:
        await context.bot.send_message(chat_id=int(user_id), text=f"Your {crypto} deposit of {format_amount(amount)} has been approved.")
    except Exception:
        pass


@guard_admin
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


@guard_admin
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


@guard_admin
async def wager_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    users = await top_wagerers(limit=10)
    lines = ["Top 10 wagerers"]
    for idx, user in enumerate(users, start=1):
        lines.append(
            f"{idx}. {display_name(user)} — wagered {format_amount(float(user['total_wagered']))} TON | games {user['games_played']} | win rate {win_rate(int(user['total_wins']), int(user['total_losses']))}%"
        )
    await update.effective_message.reply_text("\n".join(lines))


@guard_admin
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
    transactions = await list_transactions_for_user(user["_id"], limit=5)
    matches = await list_matches_for_user(user["_id"], limit=5)
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


@guard_admin
async def admin_matches_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    matches = await list_active_matches(limit=20)
    lines = [
        f"{match['_id']} {match['game']} {match['status']} {format_amount(float(match['amount']))} TON"
        for match in matches
    ] or ["No pending/active/disputed matches."]
    await update.effective_message.reply_text("\n".join(lines))


@guard_admin
async def admin_ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /admin_ban <user_id>")
        return
    user_id = context.args[0]
    from db.mongo import get_db

    db = await get_db()
    await db.users.update_one({"_id": user_id}, {"$set": {"is_banned": True}})
    pending = await cancel_pending_matches_for_user(user_id)
    for match in pending:
        await cancel_match_and_refund(match)
    await update.effective_message.reply_text(f"Banned {user_id}. Refunded {len(pending)} pending matches.")


@guard_admin
async def admin_unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /admin_unban <user_id>")
        return
    user_id = context.args[0]
    from db.mongo import get_db

    db = await get_db()
    await db.users.update_one({"_id": user_id}, {"$set": {"is_banned": False}})
    await update.effective_message.reply_text(f"Unbanned {user_id}.")
    try:
        await context.bot.send_message(chat_id=int(user_id), text="✅ Your account has been restored.")
    except Exception:
        pass


@guard_admin
async def set_fee_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if not await require_finance_enabled(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /set_fee <percent>")
        return
    updated = await set_settings_values({"withdrawal_fee_percent": float(context.args[0])})
    await update.effective_message.reply_text(f"Withdrawal fee set to {updated['withdrawal_fee_percent']}%.")


@guard_admin
async def set_min_wager_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if not await require_finance_enabled(update):
        return
    if len(context.args) != 1:
        await update.effective_message.reply_text("Usage: /set_min_wager <amount>")
        return
    amount = float(context.args[0])
    await set_settings_values({"min_wager_threshold": amount})
    await sync_vip_status_all()
    await update.effective_message.reply_text(f"VIP threshold updated to {format_amount(amount)} TON.")


@guard_admin
async def set_deposit_address_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    if not await require_finance_enabled(update):
        return
    if len(context.args) != 2:
        await update.effective_message.reply_text("Usage: /set_deposit_address <TON|USDT_BEP20|SOL> <address>")
        return
    crypto = context.args[0].upper()
    if crypto not in {"TON", "USDT_BEP20", "SOL"}:
        await update.effective_message.reply_text("Crypto must be TON, USDT_BEP20, or SOL.")
        return
    settings_doc = await get_settings_doc()
    deposit_addresses = dict(settings_doc.get("deposit_addresses", {}))
    deposit_addresses[crypto] = context.args[1]
    await set_settings_values({"deposit_addresses": deposit_addresses})
    await update.effective_message.reply_text(f"Updated {crypto} deposit address.")


@guard_admin
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


@guard_admin
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


@guard_admin
async def admin_withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    query = update.callback_query
    await query.answer()
    action, withdrawal_id = query.data.split(":", 1)
    context.args = [withdrawal_id]
    if action == "admin_withdraw_approve":
        await approve_withdrawal_command(update, context)
    elif action == "admin_withdraw_reject":
        await reject_withdrawal_command(update, context)
