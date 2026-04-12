import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
ASSETS_DIR = Path(__file__).resolve().parent / "assets"
DATA_DIR = BASE_DIR / "data"

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", os.getenv("ADMIN_USER_ID", "6204931777")).strip())
DB_PATH = os.getenv("DB_PATH", str(DATA_DIR / "pvp_bot.db")).strip()
PLATFORM_FEE_RATE = float(os.getenv("PLATFORM_FEE_RATE", "0.05"))
MIN_ENTRY_FEE = float(os.getenv("MIN_ENTRY_FEE", "0.5"))
MIN_DEPOSIT = float(os.getenv("MIN_DEPOSIT", "0.5"))
MIN_WITHDRAWAL = float(os.getenv("MIN_WITHDRAWAL", "0.5"))
TONCENTER_BASE_URL = os.getenv("TONCENTER_BASE_URL", "https://toncenter.com/api/v2").rstrip("/")
TONCENTER_API_KEY = os.getenv("TONCENTER_API_KEY", "").strip()
PLATFORM_TON_WALLET = os.getenv("PLATFORM_TON_WALLET", "").strip()
TON_WALLET_MNEMONIC = os.getenv("TON_WALLET_MNEMONIC", "").strip()
TON_WALLET_VERSION = os.getenv("TON_WALLET_VERSION", "v4r2").strip().lower()
TON_WALLET_WORKCHAIN = int(os.getenv("TON_WALLET_WORKCHAIN", "0"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
PAYMENT_CHECK_LIMIT = int(os.getenv("PAYMENT_CHECK_LIMIT", "50"))
MATCH_PAYMENT_WINDOW_MINUTES = int(os.getenv("MATCH_PAYMENT_WINDOW_MINUTES", "10"))
MATCH_RESULT_REMINDER_MINUTES = int(os.getenv("MATCH_RESULT_REMINDER_MINUTES", "30"))
MATCH_RESULT_DISPUTE_MINUTES = int(os.getenv("MATCH_RESULT_DISPUTE_MINUTES", "60"))
CHESS_RESULT_SELECTION = os.getenv("CHESS_RESULT_SELECTION", "latest").strip().lower()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)
logger = logging.getLogger("pvp_bot")
