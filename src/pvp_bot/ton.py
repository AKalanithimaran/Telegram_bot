import uuid
from typing import Any, Optional

import httpx
from telegram.ext import Application
from tonsdk.contract.wallet import WalletVersionEnum, Wallets
from tonsdk.utils import Address, bytes_to_b64str, to_nano

from .config import (
    HTTP_TIMEOUT,
    MIN_DEPOSIT,
    PAYMENT_CHECK_LIMIT,
    PLATFORM_TON_WALLET,
    TONCENTER_API_KEY,
    TONCENTER_BASE_URL,
    TON_WALLET_MNEMONIC,
    TON_WALLET_VERSION,
    TON_WALLET_WORKCHAIN,
    logger,
)
from .database import adjust_user_balances, create_transaction, get_match, get_user, mark_processed_tx, processed_tx_exists, set_match_status
from .telegram_helpers import safe_send
from .utils import format_ton


def toncenter_headers() -> dict[str, str]:
    return {"X-API-Key": TONCENTER_API_KEY} if TONCENTER_API_KEY else {}


async def toncenter_get(path: str, params: Optional[dict[str, Any]] = None) -> Any:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=toncenter_headers()) as client:
        response = await client.get(f"{TONCENTER_BASE_URL}/{path.lstrip('/')}", params=params or {})
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(payload.get("description") or "TonCenter request failed.")
        return payload.get("result")


async def toncenter_post(path: str, json_body: dict[str, Any]) -> Any:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=toncenter_headers()) as client:
        response = await client.post(f"{TONCENTER_BASE_URL}/{path.lstrip('/')}", json=json_body)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(payload.get("description") or "TonCenter request failed.")
        return payload.get("result")


async def fetch_platform_transactions() -> list[dict[str, Any]]:
    if not PLATFORM_TON_WALLET:
        return []
    result = await toncenter_get("getTransactions", {"address": PLATFORM_TON_WALLET, "limit": PAYMENT_CHECK_LIMIT})
    return result if isinstance(result, list) else []


async def fetch_platform_wallet_balance() -> float:
    if not PLATFORM_TON_WALLET:
        return 0.0
    result = await toncenter_get("getAddressBalance", {"address": PLATFORM_TON_WALLET})
    return round(int(result) / 1_000_000_000, 8)


async def fetch_wallet_seqno(wallet_address: str) -> int:
    result = await toncenter_get("getWalletInformation", {"address": wallet_address})
    try:
        return int(result.get("seqno", 0))
    except (TypeError, ValueError):
        return 0



def wallet_version_enum() -> WalletVersionEnum:
    return {"v3r2": WalletVersionEnum.v3r2, "v4r2": WalletVersionEnum.v4r2}.get(TON_WALLET_VERSION, WalletVersionEnum.v4r2)


async def send_ton_withdrawal(amount: float, destination: str, comment: str) -> str:
    if not TON_WALLET_MNEMONIC:
        raise RuntimeError("TON_WALLET_MNEMONIC is not configured.")
    mnemonics = [word for word in TON_WALLET_MNEMONIC.split() if word]
    if len(mnemonics) < 12:
        raise RuntimeError("TON_WALLET_MNEMONIC is invalid.")
    try:
        Address(destination)
    except Exception as exc:
        raise RuntimeError("Invalid TON address.") from exc
    _mn, _pub, _priv, wallet = Wallets.from_mnemonics(mnemonics, wallet_version_enum(), TON_WALLET_WORKCHAIN)
    wallet_address = wallet.address.to_string(True, True, True)
    seqno = await fetch_wallet_seqno(wallet_address)
    message = wallet.create_transfer_message(destination, to_nano(amount, "ton"), seqno, payload=comment, send_mode=3)
    boc = bytes_to_b64str(message["message"].to_boc(False))
    result = await toncenter_post("sendBoc", {"boc": boc})
    if isinstance(result, str) and result:
        return result
    if isinstance(result, dict):
        for key in ("hash", "tx_hash", "@extra"):
            if result.get(key):
                return str(result[key])
    return f"withdraw:{uuid.uuid4().hex}"



def extract_tx_hash(tx: dict[str, Any]) -> Optional[str]:
    tx_id = tx.get("transaction_id") or {}
    return tx.get("hash") or tx_id.get("hash")



def extract_tx_comment(tx: dict[str, Any]) -> str:
    in_msg = tx.get("in_msg") or {}
    for candidate in (in_msg.get("message"), tx.get("comment")):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    msg_data = in_msg.get("msg_data") or {}
    for key in ("text", "body", "comment"):
        value = msg_data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""



def extract_tx_amount(tx: dict[str, Any]) -> float:
    in_msg = tx.get("in_msg") or {}
    raw_value = in_msg.get("value") or tx.get("value") or 0
    try:
        return round(int(raw_value) / 1_000_000_000, 8)
    except (TypeError, ValueError):
        return 0.0



def transaction_is_incoming(tx: dict[str, Any]) -> bool:
    return bool((tx.get("in_msg") or {}).get("source"))


async def process_deposit_tx(application: Application, tx_hash: str, user_id: int, amount: float) -> None:
    user = get_user(user_id)
    if not user:
        logger.info("Deposit memo for unknown user %s", user_id)
        create_transaction(tx_hash, user_id, amount, "deposit", "failed")
        mark_processed_tx(tx_hash)
        return
    if amount + 1e-9 < MIN_DEPOSIT:
        create_transaction(tx_hash, user_id, amount, "deposit", "failed")
        mark_processed_tx(tx_hash)
        await safe_send(application.bot, user_id, f"⚠️ Deposit received but below minimum ({format_ton(MIN_DEPOSIT)} TON). It was not credited.")
        return
    adjust_user_balances(user_id, wallet_delta=amount)
    create_transaction(tx_hash, user_id, amount, "deposit", "confirmed")
    mark_processed_tx(tx_hash)
    await safe_send(application.bot, user_id, f"✅ Deposit confirmed! +{format_ton(amount)} TON credited to your wallet.")
    logger.info("Deposit credited: user=%s amount=%s tx=%s", user_id, amount, tx_hash)


async def process_match_payment_tx(application: Application, tx_hash: str, match_id: int, amount: float, post_waiting_challenge) -> None:
    match = get_match(match_id)
    if not match or match["status"] != "pending_payment":
        create_transaction(tx_hash, match["player1"] if match else None, amount, "match_entry", "failed")
        mark_processed_tx(tx_hash)
        return
    expected = float(match["entry_fee"] or 0)
    if amount + 1e-9 < expected:
        create_transaction(tx_hash, match["player1"], amount, "match_entry", "failed")
        mark_processed_tx(tx_hash)
        await safe_send(application.bot, match["player1"], f"⚠️ Match payment for #{match_id} was below the required {format_ton(expected)} TON.")
        return
    set_match_status(match_id, "waiting", locked_amount=expected, player1_pay_mode="external", player1_paid=expected)
    create_transaction(tx_hash, match["player1"], amount, "match_entry", "confirmed")
    mark_processed_tx(tx_hash)
    await safe_send(application.bot, match["player1"], f"✅ Match payment confirmed for challenge #{match_id}. Your challenge is now live.")
    await post_waiting_challenge(application.bot, match_id)
    logger.info("Match payment confirmed: match=%s amount=%s tx=%s", match_id, amount, tx_hash)
