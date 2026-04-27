import logging
import os
from dataclasses import dataclass
from typing import Final

from dotenv import load_dotenv

load_dotenv()


def _parse_admin_ids(value: str) -> set[int]:
    admin_ids: set[int] = set()
    for chunk in value.split(","):
        chunk = chunk.strip()
        if chunk.isdigit():
            admin_ids.add(int(chunk))
    return admin_ids


def _parse_bool(value: str, default: bool = False) -> bool:
    normalized = value.strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "on"}


def _parse_int(value: str, default: int, minimum: int = 1) -> int:
    raw = value.strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return parsed if parsed >= minimum else minimum


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    mongo_uri: str
    admin_ids: set[int]
    ton_deposit_address: str
    ton_api_key: str
    toncenter_api_url: str
    webhook_url: str
    withdrawal_fee_percent: float
    min_wager_threshold: float
    port: int
    webhook_secret: str
    app_base_url: str
    request_timeout: float
    webhook_path: str
    app_env: str
    ton_enabled: bool
    sandbox_mode: bool
    app_role: str
    max_update_concurrency: int
    telegram_send_concurrency: int
    rate_limit_per_user: int


def load_settings() -> Settings:
    webhook_url = os.getenv("WEBHOOK_URL", "").strip().rstrip("/")
    app_env = os.getenv("APP_ENV", "production").strip().lower() or "production"
    ton_enabled = _parse_bool(os.getenv("ENABLE_TON", ""), default=(app_env != "development"))
    sandbox_mode = _parse_bool(os.getenv("ENABLE_SANDBOX", ""), default=(app_env == "development"))
    app_role = os.getenv("APP_ROLE", "web").strip().lower() or "web"
    if app_role not in {"web", "worker"}:
        app_role = "web"
    return Settings(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        mongo_uri=os.getenv("MONGO_URI", "").strip(),
        admin_ids=_parse_admin_ids(os.getenv("ADMIN_IDS", "")),
        ton_deposit_address=os.getenv("TON_DEPOSIT_ADDRESS", "").strip(),
        ton_api_key=os.getenv("TON_API_KEY", "").strip(),
        toncenter_api_url=os.getenv("TONCENTER_API_URL", "https://toncenter.com/api/v2").strip().rstrip("/"),
        webhook_url=webhook_url,
        withdrawal_fee_percent=float(os.getenv("WITHDRAWAL_FEE_PERCENT", "5").strip() or 5),
        min_wager_threshold=float(os.getenv("MIN_WAGER_THRESHOLD", "1000").strip() or 1000),
        port=int(os.getenv("PORT", "8000").strip() or 8000),
        webhook_secret=os.getenv("WEBHOOK_SECRET", "").strip(),
        app_base_url=webhook_url,
        request_timeout=float(os.getenv("HTTP_TIMEOUT", "20").strip() or 20),
        webhook_path="/webhook",
        app_env=app_env,
        ton_enabled=ton_enabled,
        sandbox_mode=sandbox_mode,
        app_role=app_role,
        max_update_concurrency=_parse_int(os.getenv("MAX_UPDATE_CONCURRENCY", ""), default=200, minimum=1),
        telegram_send_concurrency=_parse_int(os.getenv("TELEGRAM_SEND_CONCURRENCY", ""), default=80, minimum=1),
        rate_limit_per_user=_parse_int(os.getenv("RATE_LIMIT_PER_USER", ""), default=20, minimum=1),
    )


settings: Final[Settings] = load_settings()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("telegram_gambling_bot")
