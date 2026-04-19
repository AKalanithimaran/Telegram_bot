from __future__ import annotations

import asyncio

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

from config import logger, settings
from utils import utcnow


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
                client = AsyncIOMotorClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
                await client.admin.command("ping")
                self.client = client
                self.db = client.get_default_database()
                if self.db is None:
                    self.db = client["telegram_gambling_bot"]
                await self.ensure_indexes()
                await self.ensure_defaults()
                logger.info("MongoDB connected on attempt %s", attempt)
                return self.db
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

    async def ensure_indexes(self) -> None:
        db = self._require_db()
        await db.users.create_index([("username", ASCENDING)], sparse=True)
        await db.users.create_index([("total_wagered", DESCENDING)])
        await db.matches.create_index([("status", ASCENDING), ("created_at", DESCENDING)])
        await db.matches.create_index([("challenger_id", ASCENDING), ("opponent_id", ASCENDING)])
        await db.transactions.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
        await db.transactions.create_index([("ton_lt", ASCENDING)], unique=True, sparse=True)
        await db.pending_withdrawals.create_index([("status", ASCENDING), ("requested_at", DESCENDING)])

    async def ensure_defaults(self) -> None:
        db = self._require_db()
        await db.house.update_one(
            {"_id": "singleton"},
            {
                "$setOnInsert": {
                    "balance": 0.0,
                    "total_fees_collected": 0.0,
                    "total_deposited": 0.0,
                    "total_withdrawn": 0.0,
                    "created_at": utcnow(),
                }
            },
            upsert=True,
        )
        await db.settings.update_one(
            {"_id": "singleton"},
            {
                "$setOnInsert": {
                    "withdrawal_fee_percent": settings.withdrawal_fee_percent,
                    "min_wager_threshold": settings.min_wager_threshold,
                    "deposit_addresses": {
                        "TON": settings.ton_deposit_address,
                        "USDT_BEP20": "",
                        "SOL": "",
                    },
                    "updated_at": utcnow(),
                }
            },
            upsert=True,
        )

    def _require_db(self) -> AsyncIOMotorDatabase:
        if self.db is None:
            raise RuntimeError("MongoDB is not connected.")
        return self.db


mongo = MongoManager()


async def get_db() -> AsyncIOMotorDatabase:
    if mongo.db is None:
        await mongo.connect()
    return mongo._require_db()
