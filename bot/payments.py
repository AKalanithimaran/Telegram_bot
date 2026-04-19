from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from config import settings
from db.models import (
    add_balance,
    add_transaction,
    create_pending_withdrawal,
    get_settings_doc,
    get_user,
    refund_balance,
    reserve_balance,
)
from services.house import add_house_deposit, add_house_fee, add_house_withdrawal
from utils import format_amount


async def credit_confirmed_deposit(
    user_id: int | str,
    amount: float,
    *,
    crypto: str,
    tx_hash: str | None,
    ton_lt: str | None = None,
) -> None:
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
    settings_doc = await get_settings_doc()
    fee_percent = float(settings_doc.get("withdrawal_fee_percent", settings.withdrawal_fee_percent))
    fee = round(amount * (fee_percent / 100), 8)
    net_amount = round(amount - fee, 8)
    held_amount = round(amount + fee, 8)
    withdrawal_id = await create_pending_withdrawal(
        {
            "user_id": str(user_id),
            "amount": float(amount),
            "fee": fee,
            "net_amount": net_amount,
            "held_amount": held_amount,
            "crypto": "TON",
            "address": address,
            "status": "pending",
            "admin_id": None,
        }
    )
    await add_transaction(user_id, "withdrawal", -held_amount, "pending", crypto="TON", address=address)
    return withdrawal_id, fee, net_amount


async def approve_withdrawal_record(withdrawal: dict, admin_id: int) -> None:
    await add_house_fee(float(withdrawal["fee"]))
    await add_house_withdrawal(float(withdrawal["net_amount"]))
    await add_transaction(
        withdrawal["user_id"],
        "withdrawal",
        -float(withdrawal.get("held_amount", withdrawal["amount"])),
        "completed",
        crypto=withdrawal.get("crypto", "TON"),
        address=withdrawal.get("address"),
        admin_id=admin_id,
        metadata={"withdrawal_id": str(withdrawal["_id"])},
    )


async def reject_withdrawal_record(withdrawal: dict, admin_id: int, reason: str | None) -> None:
    refund_amount = float(withdrawal.get("held_amount", withdrawal["amount"]))
    await add_balance(
        withdrawal["user_id"],
        refund_amount,
        reason="withdrawal_rejected",
        tx_type="withdrawal",
        admin_id=admin_id,
    )
    await add_transaction(
        withdrawal["user_id"],
        "withdrawal",
        refund_amount,
        "rejected",
        crypto=withdrawal.get("crypto", "TON"),
        address=withdrawal.get("address"),
        admin_id=admin_id,
        metadata={"withdrawal_id": str(withdrawal["_id"]), "reason": reason},
    )


async def tip_users(sender_id: int, recipient_id: int | str, amount: float) -> None:
    await add_transaction(sender_id, "tip_sent", -amount, "completed", metadata={"recipient_id": str(recipient_id)})
    await add_transaction(recipient_id, "tip_received", amount, "completed", metadata={"sender_id": str(sender_id)})


async def transfer_tip(sender_id: int | str, recipient_id: int | str, amount: float) -> bool:
    if not await reserve_balance(sender_id, amount):
        return False
    await refund_balance(recipient_id, amount)
    await tip_users(sender_id, recipient_id, amount)
    return True


async def describe_balance(user_id: int | str) -> str:
    user = await get_user(user_id)
    balance = float(user["balance"]) if user else 0.0
    return f"Current balance: {format_amount(balance)} TON"


async def notify_deposit_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, crypto: str) -> None:
    settings_doc = await get_settings_doc()
    deposit_addresses = settings_doc.get("deposit_addresses", {})
    address = deposit_addresses.get(crypto, "")
    lines = [
        f"{crypto} deposit instructions",
        f"Address: {address or 'Not configured'}",
        f"Memo/comment: your Telegram user ID `{update.effective_user.id}`",
    ]
    if crypto == "TON":
        lines.append("TON is auto-detected every 30 seconds via TonCenter.")
    else:
        lines.append("After sending, reply in private with your tx hash so admins can review it manually.")
    await update.effective_message.reply_text("\n".join(lines))
