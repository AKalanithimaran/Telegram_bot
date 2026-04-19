from __future__ import annotations

from db.mongo import get_db
from utils import utcnow


async def add_house_fee(amount: float) -> None:
    db = await get_db()
    await db.house.update_one(
        {"_id": "singleton"},
        {"$inc": {"balance": float(amount), "total_fees_collected": float(amount)}, "$set": {"updated_at": utcnow()}},
    )


async def add_house_deposit(amount: float) -> None:
    db = await get_db()
    await db.house.update_one(
        {"_id": "singleton"},
        {"$inc": {"total_deposited": float(amount)}, "$set": {"updated_at": utcnow()}},
    )


async def add_house_withdrawal(amount: float) -> None:
    db = await get_db()
    await db.house.update_one(
        {"_id": "singleton"},
        {"$inc": {"total_withdrawn": float(amount)}, "$set": {"updated_at": utcnow()}},
    )
