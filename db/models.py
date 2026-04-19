from __future__ import annotations

from typing import Any

from bson import ObjectId
from nanoid import generate
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from config import settings
from db.mongo import get_db
from utils import (
    get_cached_house,
    get_cached_settings,
    invalidate_house_cache,
    invalidate_settings_cache,
    utcnow,
)


MATCH_ID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"


async def ensure_user(user_id: int, username: str | None, first_name: str | None) -> dict[str, Any]:
    db = await get_db()
    now = utcnow()
    await db.users.update_one(
        {"_id": str(user_id)},
        {
            "$setOnInsert": {
                "_id": str(user_id),
                "balance": 0.0,
                "total_wagered": 0.0,
                "total_wins": 0,
                "total_losses": 0,
                "total_profit": 0.0,
                "games_played": 0,
                "is_banned": False,
                "is_vip": False,
                "mlbb_id": None,
                "mlbb_verified": False,
                "joined_at": now,
            },
            "$set": {
                "username": (username or "").lower() or None,
                "first_name": first_name or "",
                "last_active": now,
            },
        },
        upsert=True,
    )
    user = await db.users.find_one({"_id": str(user_id)})
    if not user:
        raise RuntimeError("Failed to ensure user record.")
    return user


async def get_user(user_id: int | str) -> dict[str, Any] | None:
    db = await get_db()
    return await db.users.find_one({"_id": str(user_id)})


async def get_user_by_username(username: str) -> dict[str, Any] | None:
    db = await get_db()
    return await db.users.find_one({"username": username.lower()})


async def require_house() -> dict[str, Any]:
    house = await get_cached_house()
    if not house:
        raise RuntimeError("House settings not initialized.")
    return house


async def get_settings_doc() -> dict[str, Any]:
    doc = await get_cached_settings()
    if not doc:
        raise RuntimeError("Settings document not initialized.")
    return doc


async def set_settings_values(values: dict[str, Any]) -> dict[str, Any]:
    db = await get_db()
    values["updated_at"] = utcnow()
    updated = await db.settings.find_one_and_update(
        {"_id": "singleton"},
        {"$set": values},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    invalidate_settings_cache()
    return updated


async def sync_vip_status_for_user(user_id: int | str) -> None:
    db = await get_db()
    settings_doc = await get_settings_doc()
    threshold = float(settings_doc.get("min_wager_threshold", settings.min_wager_threshold))
    await db.users.update_one(
        {"_id": str(user_id)},
        [
            {
                "$set": {
                    "is_vip": {
                        "$gte": ["$total_wagered", threshold],
                    }
                }
            }
        ],
    )


async def sync_vip_status_all() -> None:
    db = await get_db()
    settings_doc = await get_settings_doc()
    threshold = float(settings_doc.get("min_wager_threshold", settings.min_wager_threshold))
    await db.users.update_many({"total_wagered": {"$gte": threshold}}, {"$set": {"is_vip": True}})
    await db.users.update_many({"total_wagered": {"$lt": threshold}}, {"$set": {"is_vip": False}})


async def add_transaction(
    user_id: int | str,
    tx_type: str,
    amount: float,
    status: str,
    *,
    crypto: str = "TON",
    tx_hash: str | None = None,
    ton_lt: str | None = None,
    address: str | None = None,
    admin_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> Any:
    db = await get_db()
    doc = {
        "user_id": str(user_id),
        "type": tx_type,
        "amount": float(amount),
        "status": status,
        "crypto": crypto,
        "tx_hash": tx_hash,
        "ton_lt": ton_lt,
        "address": address,
        "admin_id": str(admin_id) if admin_id is not None else None,
        "created_at": utcnow(),
    }
    if metadata:
        doc["metadata"] = metadata
    return await db.transactions.insert_one(doc)


async def claim_ton_deposit(
    *,
    user_id: int | str,
    amount: float,
    ton_lt: str,
    tx_hash: str | None,
) -> bool:
    db = await get_db()
    try:
        await db.transactions.insert_one(
            {
                "user_id": str(user_id),
                "type": "deposit",
                "amount": float(amount),
                "status": "completed",
                "crypto": "TON",
                "tx_hash": tx_hash,
                "ton_lt": ton_lt,
                "address": None,
                "admin_id": None,
                "created_at": utcnow(),
            }
        )
    except DuplicateKeyError:
        return False
    await db.users.update_one(
        {"_id": str(user_id)},
        {"$inc": {"balance": float(amount)}, "$set": {"last_active": utcnow()}},
    )
    await db.house.update_one(
        {"_id": "singleton"},
        {"$inc": {"total_deposited": float(amount)}},
    )
    invalidate_house_cache()
    return True


async def add_balance(
    user_id: int | str,
    amount: float,
    *,
    reason: str | None = None,
    tx_type: str = "house",
    admin_id: int | None = None,
) -> dict[str, Any] | None:
    db = await get_db()
    updated = await db.users.find_one_and_update(
        {"_id": str(user_id)},
        {"$inc": {"balance": float(amount)}, "$set": {"last_active": utcnow()}},
        return_document=ReturnDocument.AFTER,
    )
    if updated:
        await add_transaction(
            user_id,
            tx_type,
            amount,
            "completed",
            admin_id=admin_id,
            metadata={"reason": reason} if reason else None,
        )
    return updated


async def reserve_balance(user_id: int | str, amount: float) -> bool:
    db = await get_db()
    result = await db.users.find_one_and_update(
        {"_id": str(user_id), "balance": {"$gte": float(amount)}},
        {"$inc": {"balance": -float(amount)}, "$set": {"last_active": utcnow()}},
        return_document=ReturnDocument.AFTER,
    )
    return result is not None


async def admin_force_deduct_balance(user_id: int | str, amount: float) -> dict[str, Any] | None:
    db = await get_db()
    return await db.users.find_one_and_update(
        {"_id": str(user_id)},
        {"$inc": {"balance": -float(amount)}, "$set": {"last_active": utcnow()}},
        return_document=ReturnDocument.AFTER,
    )


async def refund_balance(user_id: int | str, amount: float) -> None:
    db = await get_db()
    await db.users.update_one(
        {"_id": str(user_id)},
        {"$inc": {"balance": float(amount)}, "$set": {"last_active": utcnow()}},
    )


async def increment_wager_stats(user_id: int | str, amount: float) -> None:
    db = await get_db()
    await db.users.update_one(
        {"_id": str(user_id)},
        {"$inc": {"total_wagered": float(amount)}, "$set": {"last_active": utcnow()}},
    )
    await sync_vip_status_for_user(user_id)


async def record_game_result(
    winner_id: int | str,
    loser_id: int | str,
    amount: float,
    payout: float,
) -> None:
    db = await get_db()
    await db.users.update_one(
        {"_id": str(winner_id)},
        {
            "$inc": {
                "balance": float(payout),
                "total_wins": 1,
                "games_played": 1,
                "total_profit": round(float(payout) - float(amount), 8),
            },
            "$set": {"last_active": utcnow()},
        },
    )
    await db.users.update_one(
        {"_id": str(loser_id)},
        {
            "$inc": {
                "total_losses": 1,
                "games_played": 1,
                "total_profit": -float(amount),
            },
            "$set": {"last_active": utcnow()},
        },
    )


async def create_match(payload: dict[str, Any]) -> dict[str, Any]:
    db = await get_db()
    payload.setdefault("message_id", 0)
    payload.setdefault("challenger_roll", None)
    payload.setdefault("opponent_roll", None)
    payload.setdefault("fen", "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
    payload.setdefault("turn", "white")
    payload.setdefault("challenger_color", "white")
    payload.setdefault("opponent_color", "black")
    payload.setdefault("move_history", [])
    payload["_id"] = generate(MATCH_ID_ALPHABET, 8)
    payload["created_at"] = utcnow()
    payload["completed_at"] = None
    await db.matches.insert_one(payload)
    match = await db.matches.find_one({"_id": payload["_id"]})
    if not match:
        raise RuntimeError("Failed to create match.")
    return match


async def claim_match_atomically(match_id: str, opponent_id: int | str) -> dict[str, Any] | None:
    db = await get_db()
    return await db.matches.find_one_and_update(
        {"_id": match_id, "status": "pending", "opponent_id": None},
        {
            "$set": {
                "status": "active",
                "opponent_id": str(opponent_id),
            }
        },
        return_document=ReturnDocument.AFTER,
    )


async def get_match(match_id: str) -> dict[str, Any] | None:
    db = await get_db()
    return await db.matches.find_one({"_id": match_id})


async def update_match(match_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
    db = await get_db()
    return await db.matches.find_one_and_update(
        {"_id": match_id},
        {"$set": values},
        return_document=ReturnDocument.AFTER,
    )


async def create_pending_withdrawal(doc: dict[str, Any]) -> str:
    db = await get_db()
    doc["requested_at"] = utcnow()
    doc["resolved_at"] = None
    result = await db.pending_withdrawals.insert_one(doc)
    return str(result.inserted_id)


async def get_pending_withdrawal(withdrawal_id: str) -> dict[str, Any] | None:
    db = await get_db()
    try:
        _id = ObjectId(withdrawal_id)
    except Exception:
        return None
    return await db.pending_withdrawals.find_one({"_id": _id})


async def update_pending_withdrawal(withdrawal_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
    db = await get_db()
    try:
        _id = ObjectId(withdrawal_id)
    except Exception:
        return None
    return await db.pending_withdrawals.find_one_and_update(
        {"_id": _id},
        {"$set": values},
        return_document=ReturnDocument.AFTER,
    )


async def list_transactions_for_user(user_id: int | str, limit: int = 10) -> list[dict[str, Any]]:
    db = await get_db()
    cursor = db.transactions.find({"user_id": str(user_id)}).sort("created_at", -1).limit(limit)
    return await cursor.to_list(length=limit)


async def list_matches_for_user(user_id: int | str, limit: int = 10) -> list[dict[str, Any]]:
    db = await get_db()
    cursor = db.matches.find(
        {"$or": [{"challenger_id": str(user_id)}, {"opponent_id": str(user_id)}]}
    ).sort("created_at", -1).limit(limit)
    return await cursor.to_list(length=limit)


async def top_wagerers(limit: int = 10) -> list[dict[str, Any]]:
    db = await get_db()
    cursor = db.users.find({}).sort("total_wagered", -1).limit(limit)
    return await cursor.to_list(length=limit)


async def list_active_matches(limit: int = 20) -> list[dict[str, Any]]:
    db = await get_db()
    cursor = db.matches.find({"status": {"$in": ["pending", "active", "disputed"]}}).sort("created_at", -1).limit(limit)
    return await cursor.to_list(length=limit)


async def get_active_mlbb_match_for_user(user_id: int | str, match_id: str) -> dict[str, Any] | None:
    db = await get_db()
    return await db.matches.find_one(
        {
            "_id": match_id,
            "game": "mlbb",
            "status": {"$in": ["active", "disputed"]},
            "$or": [{"challenger_id": str(user_id)}, {"opponent_id": str(user_id)}],
        }
    )


async def fetch_pending_chess_matches(timeout_before: Any) -> list[dict[str, Any]]:
    db = await get_db()
    cursor = db.matches.find(
        {"game": "chess", "status": "active", "created_at": {"$lte": timeout_before}}
    )
    return await cursor.to_list(length=200)


async def fetch_stale_manual_matches(timeout_before: Any) -> list[dict[str, Any]]:
    db = await get_db()
    cursor = db.matches.find(
        {
            "game": {"$in": ["mlbb"]},
            "status": "active",
            "created_at": {"$lte": timeout_before},
        }
    )
    return await cursor.to_list(length=200)


async def admin_stats() -> dict[str, Any]:
    db = await get_db()
    total_users = await db.users.count_documents({})
    active_matches = await db.matches.count_documents({"status": "active"})
    pending_matches = await db.matches.count_documents({"status": "pending"})
    disputed_matches = await db.matches.count_documents({"status": "disputed"})
    house = await require_house()
    return {
        "total_users": total_users,
        "active_matches": active_matches,
        "pending_matches": pending_matches,
        "disputed_matches": disputed_matches,
        "house": house,
    }


async def cancel_pending_matches_for_user(user_id: int | str) -> list[dict[str, Any]]:
    db = await get_db()
    cursor = db.matches.find(
        {
            "status": "pending",
            "$or": [
                {"challenger_id": str(user_id)},
                {"opponent_id": str(user_id)},
            ],
        }
    )
    return await cursor.to_list(length=200)
