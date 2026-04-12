from datetime import datetime, timezone
from typing import Any, Optional

from telegram import Update
from telegram.constants import ChatType


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_str() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S")


def parse_db_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def format_ton(value: float) -> str:
    return f"{value:.2f}"


def parse_ton_amount(raw: str) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("Amount must be a valid number.") from exc
    if value <= 0:
        raise ValueError("Amount must be greater than 0.")
    return round(value, 8)


def username_label(username: Optional[str], user_id: Optional[int] = None) -> str:
    if username:
        return username if username.startswith("@") else f"@{username}"
    if user_id is not None:
        return f"User {user_id}"
    return "Unknown"


def safe_username(user: Optional[Any]) -> str:
    if not user:
        return ""
    if getattr(user, "username", None):
        return f"@{user.username}"
    full_name = getattr(user, "full_name", "") or ""
    return full_name[:100]


def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == ChatType.PRIVATE)


def is_group(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type in {ChatType.GROUP, ChatType.SUPERGROUP})
