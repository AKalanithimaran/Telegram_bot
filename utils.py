from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update

from config import settings

ANTI_CHEAT_WARNING = "⚠️ Cheating = permanent ban. All results are logged."
PRIVATE_ONLY_TEXT = "This command is private-only. Open the bot in DM here: {link}"


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
