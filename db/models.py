from __future__ import annotations

from typing import Any

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClientSession
from nanoid import generate
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from config import settings
from db.mongo import get_db, mongo
from utils import (
    get_cached_house,
    get_cached_settings,
    invalidate_house_cache,
    invalidate_settings_cache,
    utcnow,
)


MATCH_ID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"


def _is_vip(total_wagered: float) -> bool:
    return float(total_wagered) >= float(settings.min_wager_threshold)


async def _record_idempotency_key(key: str, payload: dict[str, Any], session: AsyncIOMotorClientSession) -> bool:
    db = await get_db()
    try:
        await db.idempotency_keys.insert_one(
            {
                "_id": key,
                "payload": payload,
                "created_at": utcnow(),
            },
            session=session,
        )
        return True
    except DuplicateKeyError:
        return False


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
    idempotency_key: str | None = None,
    session: AsyncIOMotorClientSession | None = None,
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
    if idempotency_key:
        doc["idempotency_key"] = idempotency_key
    return await db.transactions.insert_one(doc, session=session)


async def claim_ton_deposit(
    *,
    user_id: int | str,
    amount: float,
    ton_lt: str,
    tx_hash: str | None,
) -> bool:
    db = await get_db()
    async with await mongo.client.start_session() as session:
        async with session.start_transaction():
            inserted = await _record_idempotency_key(
                f"ton_deposit:{ton_lt}",
                {"user_id": str(user_id), "amount": float(amount), "ton_lt": ton_lt},
                session,
            )
            if not inserted:
                return False
            await add_transaction(
                user_id,
                "deposit",
                amount,
                "completed",
                crypto="TON",
                tx_hash=tx_hash,
                ton_lt=ton_lt,
                idempotency_key=f"ton_deposit:{ton_lt}",
                session=session,
            )
            await db.users.update_one(
                {"_id": str(user_id)},
                {"$inc": {"balance": float(amount)}, "$set": {"last_active": utcnow()}},
                session=session,
            )
            await db.house.update_one(
                {"_id": "singleton"},
                {"$inc": {"total_deposited": float(amount)}, "$set": {"updated_at": utcnow()}},
                session=session,
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
    updated = await db.users.find_one_and_update(
        {"_id": str(user_id)},
        {"$inc": {"total_wagered": float(amount)}, "$set": {"last_active": utcnow()}},
        return_document=ReturnDocument.AFTER,
    )
    if updated:
        await db.users.update_one(
            {"_id": str(user_id)},
            {"$set": {"is_vip": _is_vip(float(updated.get("total_wagered", 0.0)))}} ,
        )


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


async def create_challenge_atomic(
    user_id: int | str,
    amount: float,
    game: str,
    mode: str | None,
    dice_count: int | None,
    chat_id: int | str,
) -> dict[str, Any]:
    db = await get_db()
    match_id = generate(MATCH_ID_ALPHABET, 8)
    payload: dict[str, Any] = {
        "_id": match_id,
        "game": game,
        "mode": mode,
        "dice_count": int(dice_count) if dice_count is not None else None,
        "challenger_id": str(user_id),
        "opponent_id": None,
        "amount": float(amount),
        "status": "pending",
        "winner_id": None,
        "challenger_result": None,
        "opponent_result": None,
        "chat_id": str(chat_id),
        "message_id": 0,
        "challenger_roll": None,
        "opponent_roll": None,
        "fen": "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        "turn": "white",
        "challenger_color": "white",
        "opponent_color": "black",
        "move_history": [],
        "created_at": utcnow(),
        "completed_at": None,
    }

    async with await mongo.client.start_session() as session:
        async with session.start_transaction():
            if not settings.sandbox_mode:
                updated_user = await db.users.find_one_and_update(
                    {"_id": str(user_id), "balance": {"$gte": float(amount)}},
                    {
                        "$inc": {"balance": -float(amount), "total_wagered": float(amount)},
                        "$set": {"last_active": utcnow()},
                    },
                    return_document=ReturnDocument.AFTER,
                    session=session,
                )
                if not updated_user:
                    raise ValueError("Insufficient balance.")
                await db.users.update_one(
                    {"_id": str(user_id)},
                    {"$set": {"is_vip": _is_vip(float(updated_user.get("total_wagered", 0.0)))}},
                    session=session,
                )
                await add_transaction(
                    user_id,
                    "game_loss",
                    -float(amount),
                    "pending",
                    metadata={"stage": "escrow", "match_id": match_id},
                    idempotency_key=f"challenge_create:{match_id}:{user_id}",
                    session=session,
                )
            await db.matches.insert_one(payload, session=session)

    match = await db.matches.find_one({"_id": match_id})
    if not match:
        raise RuntimeError("Failed to create match.")
    return match


async def claim_and_activate_match_atomic(match_id: str, opponent_id: int | str) -> dict[str, Any] | None:
    db = await get_db()
    async with await mongo.client.start_session() as session:
        async with session.start_transaction():
            claimed = await db.matches.find_one_and_update(
                {"_id": match_id, "status": "pending", "opponent_id": None},
                {"$set": {"status": "active", "opponent_id": str(opponent_id)}},
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            if claimed is None:
                return None
            amount = float(claimed["amount"])
            if settings.sandbox_mode:
                return claimed

            updated_user = await db.users.find_one_and_update(
                {"_id": str(opponent_id), "balance": {"$gte": amount}},
                {
                    "$inc": {"balance": -amount, "total_wagered": amount},
                    "$set": {"last_active": utcnow()},
                },
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            if not updated_user:
                raise ValueError("Insufficient balance.")

            await db.users.update_one(
                {"_id": str(opponent_id)},
                {"$set": {"is_vip": _is_vip(float(updated_user.get("total_wagered", 0.0)))}},
                session=session,
            )
            await add_transaction(
                opponent_id,
                "game_loss",
                -amount,
                "pending",
                metadata={"stage": "escrow", "match_id": match_id},
                idempotency_key=f"challenge_accept:{match_id}:{opponent_id}",
                session=session,
            )
            return claimed


async def settle_match_atomic(match_id: str, winner_id: str, payout: float, fee: float) -> dict[str, Any] | None:
    db = await get_db()
    key = f"match_settle:{match_id}"
    async with await mongo.client.start_session() as session:
        async with session.start_transaction():
            inserted = await _record_idempotency_key(key, {"match_id": match_id, "winner_id": str(winner_id)}, session)
            if not inserted:
                return await db.matches.find_one({"_id": match_id}, session=session)

            match = await db.matches.find_one(
                {
                    "_id": match_id,
                    "status": {"$in": ["active", "disputed"]},
                    "winner_id": None,
                },
                session=session,
            )
            if not match:
                return await db.matches.find_one({"_id": match_id}, session=session)

            loser_id = match["opponent_id"] if str(winner_id) == str(match["challenger_id"]) else match["challenger_id"]
            amount = float(match["amount"])

            if not settings.sandbox_mode:
                await db.users.update_one(
                    {"_id": str(winner_id)},
                    {
                        "$inc": {
                            "balance": float(payout),
                            "total_wins": 1,
                            "games_played": 1,
                            "total_profit": round(float(payout) - amount, 8),
                        },
                        "$set": {"last_active": utcnow()},
                    },
                    session=session,
                )
                await db.users.update_one(
                    {"_id": str(loser_id)},
                    {
                        "$inc": {
                            "total_losses": 1,
                            "games_played": 1,
                            "total_profit": -amount,
                        },
                        "$set": {"last_active": utcnow()},
                    },
                    session=session,
                )
                await db.house.update_one(
                    {"_id": "singleton"},
                    {
                        "$inc": {"balance": float(fee), "total_fees_collected": float(fee)},
                        "$set": {"updated_at": utcnow()},
                    },
                    session=session,
                )

            updated = await db.matches.find_one_and_update(
                {"_id": match_id},
                {
                    "$set": {
                        "status": "completed",
                        "winner_id": str(winner_id),
                        "completed_at": utcnow(),
                    }
                },
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            return updated


async def cancel_match_and_refund_atomic(match_id: str) -> dict[str, Any] | None:
    db = await get_db()
    key = f"match_refund:{match_id}"
    async with await mongo.client.start_session() as session:
        async with session.start_transaction():
            inserted = await _record_idempotency_key(key, {"match_id": match_id}, session)
            if not inserted:
                return await db.matches.find_one({"_id": match_id}, session=session)

            match = await db.matches.find_one({"_id": match_id}, session=session)
            if not match or match.get("status") == "completed":
                return match

            previous_status = str(match.get("status", ""))
            updated = await db.matches.find_one_and_update(
                {"_id": match_id, "status": {"$ne": "completed"}},
                {"$set": {"status": "cancelled", "completed_at": utcnow()}},
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            if not updated:
                return await db.matches.find_one({"_id": match_id}, session=session)

            if not settings.sandbox_mode:
                amount = float(match["amount"])
                await db.users.update_one(
                    {"_id": str(match["challenger_id"])},
                    {"$inc": {"balance": amount}, "$set": {"last_active": utcnow()}},
                    session=session,
                )
                if previous_status in {"active", "disputed"} and match.get("opponent_id"):
                    await db.users.update_one(
                        {"_id": str(match["opponent_id"])},
                        {"$inc": {"balance": amount}, "$set": {"last_active": utcnow()}},
                        session=session,
                    )
            return updated


async def transfer_tip_atomic(
    sender_id: int | str,
    recipient_id: int | str,
    amount: float,
    *,
    idempotency_key: str,
) -> bool:
    db = await get_db()
    async with await mongo.client.start_session() as session:
        async with session.start_transaction():
            inserted = await _record_idempotency_key(
                idempotency_key,
                {
                    "sender_id": str(sender_id),
                    "recipient_id": str(recipient_id),
                    "amount": float(amount),
                },
                session,
            )
            if not inserted:
                return True

            reserved = await db.users.find_one_and_update(
                {"_id": str(sender_id), "balance": {"$gte": float(amount)}},
                {"$inc": {"balance": -float(amount)}, "$set": {"last_active": utcnow()}},
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            if not reserved:
                raise ValueError("Insufficient balance.")

            await db.users.update_one(
                {"_id": str(recipient_id)},
                {"$inc": {"balance": float(amount)}, "$set": {"last_active": utcnow()}},
                session=session,
            )
            await add_transaction(
                sender_id,
                "tip_sent",
                -float(amount),
                "completed",
                metadata={"recipient_id": str(recipient_id)},
                idempotency_key=f"{idempotency_key}:sender",
                session=session,
            )
            await add_transaction(
                recipient_id,
                "tip_received",
                float(amount),
                "completed",
                metadata={"sender_id": str(sender_id)},
                idempotency_key=f"{idempotency_key}:recipient",
                session=session,
            )
    return True


async def create_pending_withdrawal_atomic(
    user_id: int | str,
    amount: float,
    fee: float,
    net_amount: float,
    held_amount: float,
    address: str,
) -> tuple[str, float, float]:
    db = await get_db()
    async with await mongo.client.start_session() as session:
        async with session.start_transaction():
            reserved = await db.users.find_one_and_update(
                {"_id": str(user_id), "balance": {"$gte": float(held_amount)}},
                {"$inc": {"balance": -float(held_amount)}, "$set": {"last_active": utcnow()}},
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            if not reserved:
                raise ValueError("Insufficient balance.")

            doc = {
                "user_id": str(user_id),
                "amount": float(amount),
                "fee": float(fee),
                "net_amount": float(net_amount),
                "held_amount": float(held_amount),
                "crypto": "TON",
                "address": address,
                "status": "pending",
                "admin_id": None,
                "requested_at": utcnow(),
                "resolved_at": None,
            }
            result = await db.pending_withdrawals.insert_one(doc, session=session)
            withdrawal_id = str(result.inserted_id)
            await add_transaction(
                user_id,
                "withdrawal",
                -float(held_amount),
                "pending",
                crypto="TON",
                address=address,
                metadata={"withdrawal_id": withdrawal_id},
                idempotency_key=f"withdrawal_request:{withdrawal_id}",
                session=session,
            )
            return withdrawal_id, fee, net_amount


async def resolve_withdrawal_atomic(
    withdrawal_id: str,
    *,
    admin_id: int,
    approve: bool,
    reason: str | None = None,
) -> dict[str, Any] | None:
    db = await get_db()
    try:
        _id = ObjectId(withdrawal_id)
    except Exception:
        return None
    action = "approve" if approve else "reject"
    key = f"withdrawal_resolve:{withdrawal_id}:{action}"
    async with await mongo.client.start_session() as session:
        async with session.start_transaction():
            inserted = await _record_idempotency_key(key, {"withdrawal_id": withdrawal_id, "action": action}, session)
            if not inserted:
                return await db.pending_withdrawals.find_one({"_id": _id}, session=session)

            updated = await db.pending_withdrawals.find_one_and_update(
                {"_id": _id, "status": "pending"},
                {
                    "$set": {
                        "status": "approved" if approve else "rejected",
                        "resolved_at": utcnow(),
                        "admin_id": str(admin_id),
                    }
                },
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            if not updated:
                return await db.pending_withdrawals.find_one({"_id": _id}, session=session)

            if settings.sandbox_mode:
                return updated

            if approve:
                fee = float(updated.get("fee", 0.0))
                net_amount = float(updated.get("net_amount", 0.0))
                await db.house.update_one(
                    {"_id": "singleton"},
                    {
                        "$inc": {
                            "balance": fee,
                            "total_fees_collected": fee,
                            "total_withdrawn": net_amount,
                        },
                        "$set": {"updated_at": utcnow()},
                    },
                    session=session,
                )
                await add_transaction(
                    updated["user_id"],
                    "withdrawal",
                    -float(updated.get("held_amount", updated["amount"])),
                    "completed",
                    crypto=updated.get("crypto", "TON"),
                    address=updated.get("address"),
                    admin_id=admin_id,
                    metadata={"withdrawal_id": str(updated["_id"])},
                    idempotency_key=key,
                    session=session,
                )
            else:
                refund_amount = float(updated.get("held_amount", updated["amount"]))
                await db.users.update_one(
                    {"_id": str(updated["user_id"])},
                    {"$inc": {"balance": refund_amount}, "$set": {"last_active": utcnow()}},
                    session=session,
                )
                await add_transaction(
                    updated["user_id"],
                    "withdrawal",
                    refund_amount,
                    "rejected",
                    crypto=updated.get("crypto", "TON"),
                    address=updated.get("address"),
                    admin_id=admin_id,
                    metadata={"withdrawal_id": str(updated["_id"]), "reason": reason},
                    idempotency_key=key,
                    session=session,
                )
            return updated


async def apply_chess_move_atomic(match_id: str, user_id: str, move: str, fen: str) -> tuple[dict[str, Any] | None, str | None]:
    db = await get_db()
    match = await db.matches.find_one({"_id": match_id})
    if not match or match.get("game") != "chess" or match.get("status") != "active":
        return None, "invalid_match"

    turn = str(match.get("turn", "white"))
    challenger_color = str(match.get("challenger_color", "white"))
    expected_user = str(match["challenger_id"]) if turn == challenger_color else str(match["opponent_id"])
    if str(user_id) != expected_user:
        return None, "not_your_turn"

    new_turn = "black" if turn == "white" else "white"
    updated = await db.matches.find_one_and_update(
        {
            "_id": match_id,
            "status": "active",
            "game": "chess",
            "turn": turn,
        },
        {
            "$set": {"fen": fen, "turn": new_turn},
            "$push": {"move_history": move},
        },
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        return None, "stale_turn"
    return updated, None


async def acquire_job_lock(job_name: str, owner_id: str, lease_seconds: int = 50) -> bool:
    db = await get_db()
    now = utcnow()
    expires_at = now.timestamp() + max(5, int(lease_seconds))
    result = await db.job_locks.find_one_and_update(
        {
            "_id": str(job_name),
            "$or": [
                {"expires_at": {"$lte": now.timestamp()}},
                {"owner_id": owner_id},
                {"owner_id": {"$exists": False}},
            ],
        },
        {
            "$set": {
                "owner_id": owner_id,
                "expires_at": expires_at,
                "updated_at": now,
            }
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return bool(result and result.get("owner_id") == owner_id)


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
