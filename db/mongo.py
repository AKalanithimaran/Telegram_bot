from __future__ import annotations

import asyncio

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from config import logger, settings


async def ensure_indexes(db: AsyncIOMotorDatabase) -> None:
    await db.users.create_index("username", sparse=True)
    await db.users.create_index("total_wagered")
    await db.users.create_index("is_banned")
    await db.transactions.create_index("user_id")
    await db.transactions.create_index("ton_lt", unique=True, sparse=True)
    await db.transactions.create_index([("user_id", 1), ("created_at", -1)])
    await db.matches.create_index("status")
    await db.matches.create_index("challenger_id")
    await db.matches.create_index("opponent_id")
    await db.matches.create_index([("status", 1), ("created_at", -1)])
    await db.matches.create_index([("game", 1), ("status", 1), ("created_at", -1)])
    await db.pending_withdrawals.create_index("user_id")
    await db.pending_withdrawals.create_index("status")


async def init_singletons(db: AsyncIOMotorDatabase) -> None:
    await db.house.update_one(
        {"_id": "singleton"},
        {
            "$setOnInsert": {
                "_id": "singleton",
                "balance": 0.0,
                "total_fees_collected": 0.0,
                "total_deposited": 0.0,
                "total_withdrawn": 0.0,
            }
        },
        upsert=True,
    )
    await db.settings.update_one(
        {"_id": "singleton"},
        {
            "$setOnInsert": {
                "_id": "singleton",
                "withdrawal_fee_percent": settings.withdrawal_fee_percent,
                "min_wager_threshold": settings.min_wager_threshold,
                "deposit_addresses": {
                    "TON": settings.ton_deposit_address,
                    "USDT_BEP20": "",
                    "SOL": "",
                },
            }
        },
        upsert=True,
    )


class MongoManager:
    def __init__(self) -> None:
        self.client: AsyncIOMotorClient | None = None
        self.db: AsyncIOMotorDatabase | None = None

    async def connect(self, retries: int = 5, delay_seconds: float = 2.0) -> AsyncIOMotorDatabase:
        if self.db is not None:
            return self.db
        if not settings.mongo_uri:
            raise RuntimeError("MONGO_URI is missing.")
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                client = AsyncIOMotorClient(
                    settings.mongo_uri,
                    maxPoolSize=50,
                    minPoolSize=5,
                    maxIdleTimeMS=30000,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                )
                await client.admin.command("ping")
                db = client.get_default_database()
                if db is None:
                    db = client["telegram_gambling_bot"]
                await ensure_indexes(db)
                await init_singletons(db)
                self.client = client
                self.db = db
                logger.info("MongoDB connected on attempt %s", attempt)
                return db
            except Exception as exc:
                last_error = exc
                logger.warning("MongoDB connection attempt %s failed: %s", attempt, exc)
                await asyncio.sleep(delay_seconds)
        raise RuntimeError(f"MongoDB connection failed after {retries} attempts: {last_error}") from last_error

    async def close(self) -> None:
        if self.client is not None:
            self.client.close()
        self.client = None
        self.db = None

    def require_db(self) -> AsyncIOMotorDatabase:
        if self.db is None:
            raise RuntimeError("MongoDB is not connected.")
        return self.db


mongo = MongoManager()


async def get_db() -> AsyncIOMotorDatabase:
    if mongo.db is None:
        await mongo.connect()
    return mongo.require_db()
