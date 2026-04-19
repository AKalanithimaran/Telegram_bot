from __future__ import annotations

from typing import Any

import httpx

from config import logger, settings


def ton_headers() -> dict[str, str]:
    return {"X-API-Key": settings.ton_api_key} if settings.ton_api_key else {}


async def ton_get(path: str, params: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=settings.request_timeout, headers=ton_headers()) as client:
        response = await client.get(f"{settings.toncenter_api_url}/{path.lstrip('/')}", params=params or {})
        response.raise_for_status()
        payload = response.json()
    if not payload.get("ok", True):
        raise RuntimeError(payload.get("description") or "TonCenter request failed.")
    return payload.get("result")


async def fetch_recent_transactions(limit: int = 20) -> list[dict[str, Any]]:
    if not settings.ton_enabled or not settings.ton_deposit_address:
        return []
    params: dict[str, Any] = {"address": settings.ton_deposit_address, "limit": limit}
    if settings.ton_api_key:
        params["api_key"] = settings.ton_api_key
    result = await ton_get("getTransactions", params)
    return result if isinstance(result, list) else []


def extract_ton_lt(tx: dict[str, Any]) -> str | None:
    tx_id = tx.get("transaction_id") or {}
    return str(tx_id.get("lt")) if tx_id.get("lt") is not None else None


def extract_amount(tx: dict[str, Any]) -> float:
    in_msg = tx.get("in_msg") or {}
    raw = in_msg.get("value") or tx.get("value") or 0
    try:
        return round(int(raw) / 1_000_000_000, 8)
    except Exception:
        return 0.0


def extract_comment(tx: dict[str, Any]) -> str:
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


def extract_tx_hash(tx: dict[str, Any]) -> str | None:
    tx_id = tx.get("transaction_id") or {}
    if tx.get("hash"):
        return str(tx["hash"])
    if tx_id.get("hash"):
        return str(tx_id["hash"])
    return None


def is_incoming(tx: dict[str, Any]) -> bool:
    in_msg = tx.get("in_msg") or {}
    return bool(in_msg and in_msg.get("value"))


async def safe_fetch_recent_transactions(limit: int = 20) -> list[dict[str, Any]]:
    try:
        return await fetch_recent_transactions(limit=limit)
    except Exception as exc:
        logger.exception("TON polling failed: %s", exc)
        return []
