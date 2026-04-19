from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update

from config import settings

ANTI_CHEAT_WARNING = (
    "⚠️ Cheating, exploiting, or disputing valid results = "
    "permanent ban. All games are logged and audited."
)
PRIVATE_ONLY_TEXT = "This command is private-only. Open the bot in DM here: {link}"
SETTINGS_TTL = 60.0
_settings_cache: dict[str, Any] = {}
_settings_cache_ts = 0.0
_house_cache: dict[str, Any] = {}
_house_cache_ts = 0.0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def format_amount(value: float) -> str:
    return f"{float(value):.4f}".rstrip("0").rstrip(".")


def is_private_chat(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == "private")


def is_group_chat(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type in {"group", "supergroup"})


def bot_private_link(bot_username: str | None) -> str:
    if bot_username:
        return f"https://t.me/{bot_username}?start=menu"
    return settings.webhook_url or "Telegram private chat"


def private_only_markup(bot_username: str | None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Open Private Chat", url=bot_private_link(bot_username))]]
    )


def display_name(user: dict[str, Any] | None, fallback: str = "Unknown") -> str:
    if not user:
        return fallback
    username = (user.get("username") or "").strip()
    first_name = (user.get("first_name") or "").strip()
    if username:
        return f"@{username}"
    if first_name:
        return first_name
    return str(user.get("_id", fallback))


def parse_user_reference(raw: str) -> tuple[str, bool]:
    value = raw.strip()
    if value.startswith("@"):
        return value[1:].lower(), True
    return value, False


def win_rate(total_wins: int, total_losses: int) -> float:
    decided = total_wins + total_losses
    return round((total_wins / decided) * 100, 1) if decided else 0.0


async def get_cached_settings() -> dict[str, Any]:
    global _settings_cache, _settings_cache_ts
    if _settings_cache and time.monotonic() - _settings_cache_ts < SETTINGS_TTL:
        return _settings_cache
    from db.mongo import get_db

    db = await get_db()
    _settings_cache = await db.settings.find_one({"_id": "singleton"}) or {}
    _settings_cache_ts = time.monotonic()
    return _settings_cache


def invalidate_settings_cache() -> None:
    global _settings_cache, _settings_cache_ts
    _settings_cache = {}
    _settings_cache_ts = 0.0


async def get_cached_house() -> dict[str, Any]:
    global _house_cache, _house_cache_ts
    if _house_cache and time.monotonic() - _house_cache_ts < SETTINGS_TTL:
        return _house_cache
    from db.mongo import get_db

    db = await get_db()
    _house_cache = await db.house.find_one({"_id": "singleton"}) or {}
    _house_cache_ts = time.monotonic()
    return _house_cache


def invalidate_house_cache() -> None:
    global _house_cache, _house_cache_ts
    _house_cache = {}
    _house_cache_ts = 0.0
