from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from config import settings
from db.models import (
    add_balance,
    add_transaction,
    create_pending_withdrawal_atomic,
    get_settings_doc,
    get_user,
    resolve_withdrawal_atomic,
    transfer_tip_atomic,
)
from services.house import add_house_deposit
from utils import format_amount, utcnow


async def credit_confirmed_deposit(
    user_id: int | str,
    amount: float,
    *,
    crypto: str,
    tx_hash: str | None,
    ton_lt: str | None = None,
) -> None:
    if settings.sandbox_mode:
        return
    await add_balance(user_id, amount, tx_type="deposit")
    await add_house_deposit(amount)
    await add_transaction(
        user_id,
        "deposit",
        amount,
        "completed",
        crypto=crypto,
        tx_hash=tx_hash,
        ton_lt=ton_lt,
    )


async def create_withdrawal_request(user_id: int, amount: float, address: str) -> tuple[str, float, float]:
    if settings.sandbox_mode:
        raise RuntimeError("Withdrawal is disabled in sandbox mode.")
    settings_doc = await get_settings_doc()
    fee_percent = float(settings_doc.get("withdrawal_fee_percent", settings.withdrawal_fee_percent))
    fee = round(amount * (fee_percent / 100), 8)
    net_amount = round(amount - fee, 8)
    held_amount = round(amount + fee, 8)
    return await create_pending_withdrawal_atomic(
        user_id=user_id,
        amount=amount,
        fee=fee,
        net_amount=net_amount,
        held_amount=held_amount,
        address=address,
    )


async def approve_withdrawal_record(withdrawal: dict, admin_id: int) -> dict | None:
    return await resolve_withdrawal_atomic(
        str(withdrawal["_id"]),
        admin_id=admin_id,
        approve=True,
    )


async def reject_withdrawal_record(withdrawal: dict, admin_id: int, reason: str | None) -> dict | None:
    return await resolve_withdrawal_atomic(
        str(withdrawal["_id"]),
        admin_id=admin_id,
        approve=False,
        reason=reason,
    )


async def transfer_tip(
    sender_id: int | str,
    recipient_id: int | str,
    amount: float,
    *,
    idempotency_key: str | None = None,
) -> bool:
    if settings.sandbox_mode:
        return True
    key = idempotency_key or f"tip_transfer:{sender_id}:{recipient_id}:{format_amount(amount)}:{utcnow().timestamp()}"
    try:
        return await transfer_tip_atomic(sender_id, recipient_id, amount, idempotency_key=key)
    except ValueError:
        return False


async def describe_balance(user_id: int | str) -> str:
    user = await get_user(user_id)
    balance = float(user["balance"]) if user else 0.0
    return f"💰 Current balance: {format_amount(balance)} TON"


async def notify_deposit_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, crypto: str) -> None:
    settings_doc = await get_settings_doc()
    deposit_addresses = settings_doc.get("deposit_addresses", {})
    address = deposit_addresses.get(crypto, "")
    lines = [
        f"💳 {crypto} Deposit Instructions",
        f"📍 Address: {address or 'Not configured'}",
        f"🆔 Memo/comment: your Telegram user ID `{update.effective_user.id}`",
    ]
    if crypto == "TON":
        lines.append("🤖 TON is auto-detected every 30 seconds via TonCenter.")
    else:
        lines.append("📨 After sending, reply in private with your tx hash so admins can review it manually.")
    await update.effective_message.reply_text("\n".join(lines))
