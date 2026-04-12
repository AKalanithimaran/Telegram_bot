import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ChatType
from telegram.error import TelegramError
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes
from tonsdk.contract.wallet import WalletVersionEnum, Wallets
from tonsdk.utils import Address, bytes_to_b64str, to_nano

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", os.getenv("ADMIN_USER_ID", "6204931777")).strip())
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), "pvp_bot.db")).strip()
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




async def force_delete_webhook() -> None:
    if not BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await client.post(url, data={"drop_pending_updates": "true"})
            response.raise_for_status()
            logger.info("Webhook delete request sent before polling startup.")
    except Exception as exc:
        logger.warning("Failed to delete webhook via direct API call: %s", exc)

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


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# 1. Database init + all helper functions

def init_db() -> None:
    with closing(get_conn()) as conn, conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id                 INTEGER PRIMARY KEY,
                username                TEXT,
                mlbb_id                 TEXT,
                wallet_balance          REAL    DEFAULT 0,
                locked_balance          REAL    DEFAULT 0,
                ton_address             TEXT,
                is_verified             INTEGER DEFAULT 0,
                wins                    INTEGER DEFAULT 0,
                losses                  INTEGER DEFAULT 0,
                disputes                INTEGER DEFAULT 0,
                total_earned            REAL    DEFAULT 0,
                created_at              TEXT    DEFAULT (datetime('now')),
                verification_requested  INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS matches (
                match_id             INTEGER PRIMARY KEY AUTOINCREMENT,
                player1              INTEGER NOT NULL,
                player2              INTEGER,
                game                 TEXT    NOT NULL,
                amount               TEXT    NOT NULL,
                entry_fee            REAL    DEFAULT 0,
                locked_amount        REAL    DEFAULT 0,
                status               TEXT    DEFAULT 'waiting',
                result1              TEXT,
                result2              TEXT,
                winner_id            INTEGER,
                payout_sent          INTEGER DEFAULT 0,
                created_at           TEXT    DEFAULT (datetime('now')),
                group_chat_id        INTEGER,
                started_at           TEXT,
                player1_pay_mode     TEXT    DEFAULT 'wallet',
                player2_pay_mode     TEXT    DEFAULT 'wallet',
                player1_paid         REAL    DEFAULT 0,
                player2_paid         REAL    DEFAULT 0,
                challenge_message_id INTEGER,
                payment_notice_sent  INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS transactions (
                tx_hash         TEXT    PRIMARY KEY,
                user_id         INTEGER,
                amount          REAL,
                type            TEXT,
                status          TEXT    DEFAULT 'pending',
                created_at      TEXT    DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS processed_tx (
                tx_hash         TEXT    PRIMARY KEY
            );
            """
        )

        # Migrations for older databases.
        user_columns = {row[1] for row in conn.execute("PRAGMA table_info(users)")}
        if "username" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN username TEXT")
        if "wallet_balance" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN wallet_balance REAL DEFAULT 0")
        if "locked_balance" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN locked_balance REAL DEFAULT 0")
        if "ton_address" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN ton_address TEXT")
        if "is_verified" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN is_verified INTEGER DEFAULT 0")
        if "wins" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN wins INTEGER DEFAULT 0")
        if "losses" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN losses INTEGER DEFAULT 0")
        if "disputes" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN disputes INTEGER DEFAULT 0")
        if "total_earned" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN total_earned REAL DEFAULT 0")
        if "created_at" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN created_at TEXT DEFAULT (datetime('now'))")
        if "verification_requested" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN verification_requested INTEGER DEFAULT 0")

        match_columns = {row[1] for row in conn.execute("PRAGMA table_info(matches)")}
        for column_sql in [
            ("entry_fee", "ALTER TABLE matches ADD COLUMN entry_fee REAL DEFAULT 0"),
            ("locked_amount", "ALTER TABLE matches ADD COLUMN locked_amount REAL DEFAULT 0"),
            ("winner_id", "ALTER TABLE matches ADD COLUMN winner_id INTEGER"),
            ("payout_sent", "ALTER TABLE matches ADD COLUMN payout_sent INTEGER DEFAULT 0"),
            ("created_at", "ALTER TABLE matches ADD COLUMN created_at TEXT DEFAULT (datetime('now'))"),
            ("group_chat_id", "ALTER TABLE matches ADD COLUMN group_chat_id INTEGER"),
            ("started_at", "ALTER TABLE matches ADD COLUMN started_at TEXT"),
            ("player1_pay_mode", "ALTER TABLE matches ADD COLUMN player1_pay_mode TEXT DEFAULT 'wallet'"),
            ("player2_pay_mode", "ALTER TABLE matches ADD COLUMN player2_pay_mode TEXT DEFAULT 'wallet'"),
            ("player1_paid", "ALTER TABLE matches ADD COLUMN player1_paid REAL DEFAULT 0"),
            ("player2_paid", "ALTER TABLE matches ADD COLUMN player2_paid REAL DEFAULT 0"),
            ("challenge_message_id", "ALTER TABLE matches ADD COLUMN challenge_message_id INTEGER"),
            ("payment_notice_sent", "ALTER TABLE matches ADD COLUMN payment_notice_sent INTEGER DEFAULT 0"),
        ]:
            if column_sql[0] not in match_columns:
                conn.execute(column_sql[1])


def ensure_user_record(user_id: int, username: str = "") -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute(
            """
            INSERT INTO users (user_id, username, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
            """,
            (user_id, username, utc_now_str()),
        )


def sync_user_from_update(update: Update) -> None:
    if not update.effective_user:
        return
    ensure_user_record(update.effective_user.id, safe_username(update.effective_user))


def get_user(user_id: int) -> Optional[sqlite3.Row]:
    with closing(get_conn()) as conn:
        return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()


def get_match(match_id: int) -> Optional[sqlite3.Row]:
    with closing(get_conn()) as conn:
        return conn.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()


def get_recent_matches_for_user(user_id: int, limit: int = 10) -> list[sqlite3.Row]:
    with closing(get_conn()) as conn:
        return conn.execute(
            """
            SELECT * FROM matches
            WHERE player1 = ? OR player2 = ?
            ORDER BY match_id DESC
            LIMIT ?
            """,
            (user_id, user_id, limit),
        ).fetchall()


def get_user_match_stats(user_id: int) -> dict[str, float]:
    with closing(get_conn()) as conn:
        total_matches = conn.execute(
            "SELECT COUNT(*) FROM matches WHERE (player1 = ? OR player2 = ?) AND status IN ('completed', 'dispute')",
            (user_id, user_id),
        ).fetchone()[0]
        total_lost = conn.execute(
            """
            SELECT COALESCE(SUM(entry_fee), 0)
            FROM matches
            WHERE status = 'completed'
              AND payout_sent = 1
              AND winner_id IS NOT NULL
              AND winner_id != ?
              AND (player1 = ? OR player2 = ?)
            """,
            (user_id, user_id, user_id),
        ).fetchone()[0]
    return {"total_matches": total_matches, "total_lost": float(total_lost or 0)}


def create_transaction(tx_hash: str, user_id: Optional[int], amount: float, tx_type: str, status: str) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO transactions (tx_hash, user_id, amount, type, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (tx_hash, user_id, amount, tx_type, status, utc_now_str()),
        )


def processed_tx_exists(tx_hash: str) -> bool:
    with closing(get_conn()) as conn:
        return conn.execute("SELECT 1 FROM processed_tx WHERE tx_hash = ?", (tx_hash,)).fetchone() is not None


def mark_processed_tx(tx_hash: str) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute("INSERT OR IGNORE INTO processed_tx (tx_hash) VALUES (?)", (tx_hash,))


def update_user_verification(user_id: int, is_verified: int, verification_requested: int = 0) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute(
            "UPDATE users SET is_verified = ?, verification_requested = ? WHERE user_id = ?",
            (is_verified, verification_requested, user_id),
        )


def set_user_mlbb(user_id: int, mlbb_id: str) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute("UPDATE users SET mlbb_id = ? WHERE user_id = ?", (mlbb_id, user_id))


def set_user_ton_address(user_id: int, ton_address: str) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute("UPDATE users SET ton_address = ? WHERE user_id = ?", (ton_address, user_id))


def adjust_user_balances(user_id: int, wallet_delta: float = 0.0, locked_delta: float = 0.0, earned_delta: float = 0.0) -> None:
    with closing(get_conn()) as conn, conn:
        row = conn.execute(
            "SELECT wallet_balance, locked_balance, total_earned FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not row:
            raise ValueError("User not found.")
        new_wallet = round(float(row["wallet_balance"] or 0) + wallet_delta, 8)
        new_locked = round(float(row["locked_balance"] or 0) + locked_delta, 8)
        new_earned = round(float(row["total_earned"] or 0) + earned_delta, 8)
        if new_wallet < -1e-9 or new_locked < -1e-9:
            raise ValueError("Balance operation would make balances negative.")
        conn.execute(
            "UPDATE users SET wallet_balance = ?, locked_balance = ?, total_earned = ? WHERE user_id = ?",
            (max(new_wallet, 0.0), max(new_locked, 0.0), max(new_earned, 0.0), user_id),
        )


def lock_wallet_entry(user_id: int, amount: float, reference: str) -> None:
    with closing(get_conn()) as conn, conn:
        row = conn.execute("SELECT wallet_balance, locked_balance FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if not row:
            raise ValueError("User not found.")
        available = float(row["wallet_balance"] or 0)
        if available + 1e-9 < amount:
            raise ValueError("Insufficient wallet balance.")
        conn.execute(
            "UPDATE users SET wallet_balance = ?, locked_balance = ? WHERE user_id = ?",
            (round(available - amount, 8), round(float(row["locked_balance"] or 0) + amount, 8), user_id),
        )
    create_transaction(f"match_entry:{reference}:{user_id}", user_id, -amount, "match_entry", "confirmed")


def release_locked_amount(user_id: int, amount: float, credit_to_wallet: bool, reference: str) -> None:
    with closing(get_conn()) as conn, conn:
        row = conn.execute("SELECT wallet_balance, locked_balance FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if not row:
            raise ValueError("User not found.")
        new_locked = round(float(row["locked_balance"] or 0) - amount, 8)
        if new_locked < -1e-9:
            new_locked = 0.0
        new_wallet = float(row["wallet_balance"] or 0)
        if credit_to_wallet:
            new_wallet = round(new_wallet + amount, 8)
        conn.execute(
            "UPDATE users SET wallet_balance = ?, locked_balance = ? WHERE user_id = ?",
            (new_wallet, max(new_locked, 0.0), user_id),
        )
    if credit_to_wallet:
        create_transaction(f"match_payout:{reference}:{user_id}", user_id, amount, "match_payout", "confirmed")


def update_match_after_challenge(match_id: int, message_id: Optional[int]) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute(
            "UPDATE matches SET challenge_message_id = ? WHERE match_id = ?",
            (message_id, match_id),
        )


def store_match_result(match_id: int, user_id: int, result: str) -> None:
    with closing(get_conn()) as conn, conn:
        match = conn.execute("SELECT player1, player2 FROM matches WHERE match_id = ?", (match_id,)).fetchone()
        if not match:
            raise ValueError("Match not found.")
        if match["player1"] == user_id:
            conn.execute("UPDATE matches SET result1 = ? WHERE match_id = ?", (result, match_id))
        elif match["player2"] == user_id:
            conn.execute("UPDATE matches SET result2 = ? WHERE match_id = ?", (result, match_id))
        else:
            raise ValueError("You are not part of this match.")


def get_active_manual_matches(user_id: int) -> list[sqlite3.Row]:
    with closing(get_conn()) as conn:
        return conn.execute(
            """
            SELECT * FROM matches
            WHERE status = 'active'
              AND game IN ('mlbb', 'chess')
              AND (player1 = ? OR player2 = ?)
            ORDER BY match_id DESC
            """,
            (user_id, user_id),
        ).fetchall()


def set_match_status(match_id: int, status: str, **extra: Any) -> None:
    parts = ["status = ?"]
    values: list[Any] = [status]
    for key, value in extra.items():
        parts.append(f"{key} = ?")
        values.append(value)
    values.append(match_id)
    with closing(get_conn()) as conn, conn:
        conn.execute(f"UPDATE matches SET {', '.join(parts)} WHERE match_id = ?", values)


def increment_dispute_stats(match: sqlite3.Row) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute("UPDATE users SET disputes = disputes + 1 WHERE user_id = ?", (match["player1"],))
        if match["player2"]:
            conn.execute("UPDATE users SET disputes = disputes + 1 WHERE user_id = ?", (match["player2"],))


def finalize_match_payout(match_id: int, winner_id: int) -> tuple[sqlite3.Row, float]:
    with closing(get_conn()) as conn, conn:
        match = conn.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()
        if not match:
            raise ValueError("Match not found.")
        if match["payout_sent"]:
            return match, 0.0
        if winner_id not in {match["player1"], match["player2"]}:
            raise ValueError("Winner must be one of the match players.")
        entry_fee = float(match["entry_fee"] or 0)
        gross = round(entry_fee * 2, 8)
        payout = round(gross * (1 - PLATFORM_FEE_RATE), 8)

        player1_paid = float(match["player1_paid"] or 0)
        player2_paid = float(match["player2_paid"] or 0)
        if player1_paid > 0 and match["player1_pay_mode"] == "wallet":
            conn.execute(
                "UPDATE users SET locked_balance = MAX(locked_balance - ?, 0) WHERE user_id = ?",
                (player1_paid, match["player1"]),
            )
        if player2_paid > 0 and match["player2"] and match["player2_pay_mode"] == "wallet":
            conn.execute(
                "UPDATE users SET locked_balance = MAX(locked_balance - ?, 0) WHERE user_id = ?",
                (player2_paid, match["player2"]),
            )

        loser_id = match["player1"] if match["player2"] == winner_id else match["player2"]
        conn.execute(
            "UPDATE users SET wallet_balance = wallet_balance + ?, total_earned = total_earned + ?, wins = wins + 1 WHERE user_id = ?",
            (payout, payout, winner_id),
        )
        if loser_id:
            conn.execute("UPDATE users SET losses = losses + 1 WHERE user_id = ?", (loser_id,))

        conn.execute(
            """
            UPDATE matches
            SET status = 'completed', winner_id = ?, payout_sent = 1, locked_amount = 0
            WHERE match_id = ?
            """,
            (winner_id, match_id),
        )

    create_transaction(f"match_payout:{match_id}:{winner_id}", winner_id, payout, "match_payout", "confirmed")
    refreshed = get_match(match_id)
    if refreshed is None:
        raise ValueError("Match disappeared after payout.")
    return refreshed, payout


def refund_match(match_id: int) -> sqlite3.Row:
    with closing(get_conn()) as conn, conn:
        match = conn.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()
        if not match:
            raise ValueError("Match not found.")
        if match["status"] == "completed" and match["payout_sent"]:
            raise ValueError("Completed match cannot be refunded.")

        for player_key, pay_key, paid_key in [("player1", "player1_pay_mode", "player1_paid"), ("player2", "player2_pay_mode", "player2_paid")]:
            user_id = match[player_key]
            if not user_id:
                continue
            amount = float(match[paid_key] or 0)
            if amount <= 0:
                continue
            pay_mode = match[pay_key] or "wallet"
            user = conn.execute("SELECT wallet_balance, locked_balance FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if not user:
                continue
            wallet_balance = float(user["wallet_balance"] or 0)
            locked_balance = float(user["locked_balance"] or 0)
            if pay_mode == "wallet":
                locked_balance = max(locked_balance - amount, 0.0)
            wallet_balance = round(wallet_balance + amount, 8)
            conn.execute(
                "UPDATE users SET wallet_balance = ?, locked_balance = ? WHERE user_id = ?",
                (wallet_balance, locked_balance, user_id),
            )
            create_transaction(f"match_payout:refund:{match_id}:{user_id}", user_id, amount, "match_payout", "confirmed")

        conn.execute(
            "UPDATE matches SET status = 'cancelled', payout_sent = 1, locked_amount = 0 WHERE match_id = ?",
            (match_id,),
        )

    refunded = get_match(match_id)
    if refunded is None:
        raise ValueError("Match disappeared after refund.")
    return refunded


def choose_manual_result_match(user_id: int) -> Optional[sqlite3.Row]:
    matches = get_active_manual_matches(user_id)
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    if CHESS_RESULT_SELECTION == "latest":
        return matches[0]
    return None


def verification_status_text(user: sqlite3.Row) -> str:
    if int(user["is_verified"] or 0) == 1:
        return "✅ Verified"
    if int(user["is_verified"] or 0) == -1:
        return "⛔ Banned"
    if int(user["verification_requested"] or 0) == 1:
        return "⏳ Pending"
    return "❌ Not Verified"


def can_use_paid_features(user: sqlite3.Row) -> tuple[bool, str]:
    status = int(user["is_verified"] or 0)
    if status == -1:
        return False, "⛔ You have been banned. Contact admin."
    if status != 1:
        return False, "⚠️ You must be verified to use this feature.\nSend /verify to request approval from admin."
    return True, ""


def prize_pool_text(entry_fee: float) -> str:
    gross = entry_fee * 2
    net = gross * (1 - PLATFORM_FEE_RATE)
    return f"{format_ton(net)} TON"


def challenge_post_text(match: sqlite3.Row, challenger: sqlite3.Row) -> str:
    label = username_label(challenger["username"], challenger["user_id"])
    entry_fee = float(match["entry_fee"] or 0)
    if match["game"] == "mlbb":
        return (
            "🎮 MLBB 1v1 Challenge!\n\n"
            f"👤 Player: {label}\n"
            f"💰 Entry Fee: {format_ton(entry_fee)} TON\n"
            f"🏆 Prize Pool: {prize_pool_text(entry_fee)}\n\n"
            "Requirements:\n"
            "✅ Must be verified\n"
            "✅ Must have MLBB ID set\n"
            f"✅ Must have {format_ton(entry_fee)} TON in wallet\n\n"
            f"Use /accept {match['match_id']} to join!"
        )
    return (
        "🎮 PvP Challenge!\n\n"
        f"👤 Player: {label}\n"
        f"💰 Entry Fee: {format_ton(entry_fee)} TON\n"
        f"🎯 Game: {match['game']}\n"
        f"🏆 Prize Pool: {prize_pool_text(entry_fee)}\n\n"
        f"Use /accept {match['match_id']} to join!"
    )


async def safe_reply(update: Update, text: str) -> None:
    try:
        if update.message:
            await update.message.reply_text(text)
    except TelegramError as exc:
        logger.warning("Failed to reply in chat %s: %s", update.effective_chat.id if update.effective_chat else None, exc)


async def safe_send(bot, chat_id: int, text: str) -> None:
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except TelegramError as exc:
        logger.warning("Failed to send message to %s: %s", chat_id, exc)


async def pin_message_if_possible(bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.pin_chat_message(chat_id=chat_id, message_id=message_id, disable_notification=True)
    except TelegramError:
        logger.info("Could not pin message in group %s", chat_id)


async def notify_admin(bot, text: str) -> None:
    await safe_send(bot, ADMIN_ID, text)


async def require_private(update: Update) -> bool:
    if is_private(update):
        return True
    await safe_reply(update, "This command can only be used in private chat.")
    return False


async def require_group(update: Update) -> bool:
    if is_group(update):
        return True
    await safe_reply(update, "This command can only be used in group chat.")
    return False


async def require_admin_private(update: Update) -> bool:
    if not is_private(update):
        await safe_reply(update, "This command only works in private chat.")
        return False
    if not update.effective_user or update.effective_user.id != ADMIN_ID:
        await safe_reply(update, "Unauthorized.")
        return False
    return True


async def get_current_user(update: Update) -> sqlite3.Row:
    sync_user_from_update(update)
    user = get_user(update.effective_user.id)
    if user is None:
        raise RuntimeError("User record could not be created.")
    return user


async def require_verified_user(update: Update) -> Optional[sqlite3.Row]:
    user = await get_current_user(update)
    allowed, message = can_use_paid_features(user)
    if not allowed:
        await safe_reply(update, message)
        return None
    return user


async def post_waiting_challenge(bot, match_id: int) -> Optional[int]:
    match = get_match(match_id)
    if not match:
        return None
    challenger = get_user(match["player1"])
    if not challenger:
        return None
    text = challenge_post_text(match, challenger)
    try:
        sent = await bot.send_message(chat_id=match["group_chat_id"], text=text)
        update_match_after_challenge(match_id, sent.message_id)
        await pin_message_if_possible(bot, match["group_chat_id"], sent.message_id)
        return sent.message_id
    except TelegramError as exc:
        logger.warning("Failed to post challenge %s: %s", match_id, exc)
        return None


# 2. TonCenter API functions (deposit poll, withdraw send)

def toncenter_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    if TONCENTER_API_KEY:
        headers["X-API-Key"] = TONCENTER_API_KEY
    return headers


async def toncenter_get(path: str, params: Optional[dict[str, Any]] = None) -> Any:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=toncenter_headers()) as client:
        response = await client.get(f"{TONCENTER_BASE_URL}/{path.lstrip('/')}", params=params or {})
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(payload.get("description") or "TonCenter request failed.")
        return payload.get("result")


async def toncenter_post(path: str, json_body: dict[str, Any]) -> Any:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=toncenter_headers()) as client:
        response = await client.post(f"{TONCENTER_BASE_URL}/{path.lstrip('/')}", json=json_body)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(payload.get("description") or "TonCenter request failed.")
        return payload.get("result")


async def fetch_platform_transactions() -> list[dict[str, Any]]:
    if not PLATFORM_TON_WALLET:
        return []
    result = await toncenter_get("getTransactions", {"address": PLATFORM_TON_WALLET, "limit": PAYMENT_CHECK_LIMIT})
    return result if isinstance(result, list) else []


async def fetch_platform_wallet_balance() -> float:
    if not PLATFORM_TON_WALLET:
        return 0.0
    result = await toncenter_get("getAddressBalance", {"address": PLATFORM_TON_WALLET})
    return round(int(result) / 1_000_000_000, 8)


async def fetch_wallet_seqno(wallet_address: str) -> int:
    result = await toncenter_get("getWalletInformation", {"address": wallet_address})
    try:
        return int(result.get("seqno", 0))
    except (TypeError, ValueError):
        return 0



def wallet_version_enum() -> WalletVersionEnum:
    version_map = {
        "v3r2": WalletVersionEnum.v3r2,
        "v4r2": WalletVersionEnum.v4r2,
    }
    return version_map.get(TON_WALLET_VERSION, WalletVersionEnum.v4r2)


async def send_ton_withdrawal(amount: float, destination: str, comment: str) -> str:
    if not TON_WALLET_MNEMONIC:
        raise RuntimeError("TON_WALLET_MNEMONIC is not configured.")
    mnemonics = [word for word in TON_WALLET_MNEMONIC.split() if word]
    if len(mnemonics) < 12:
        raise RuntimeError("TON_WALLET_MNEMONIC is invalid.")

    try:
        Address(destination)
    except Exception as exc:
        raise RuntimeError("Invalid TON address.") from exc

    _mn, _pub, _priv, wallet = Wallets.from_mnemonics(mnemonics, wallet_version_enum(), TON_WALLET_WORKCHAIN)
    wallet_address = wallet.address.to_string(True, True, True)
    seqno = await fetch_wallet_seqno(wallet_address)
    message = wallet.create_transfer_message(
        destination,
        to_nano(amount, "ton"),
        seqno,
        payload=comment,
        send_mode=3,
    )
    boc = bytes_to_b64str(message["message"].to_boc(False))
    result = await toncenter_post("sendBoc", {"boc": boc})
    if isinstance(result, str) and result:
        return result
    if isinstance(result, dict):
        for key in ("hash", "tx_hash", "@extra"):
            value = result.get(key)
            if value:
                return str(value)
    return f"withdraw:{uuid.uuid4().hex}"



def extract_tx_hash(tx: dict[str, Any]) -> Optional[str]:
    tx_id = tx.get("transaction_id") or {}
    return tx.get("hash") or tx_id.get("hash")



def extract_tx_comment(tx: dict[str, Any]) -> str:
    in_msg = tx.get("in_msg") or {}
    for candidate in (in_msg.get("message"), tx.get("comment")):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    msg_data = in_msg.get("msg_data") or {}
    for key in ("text", "body", "comment"):
        value = msg_data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""



def extract_tx_amount(tx: dict[str, Any]) -> float:
    in_msg = tx.get("in_msg") or {}
    raw_value = in_msg.get("value") or tx.get("value") or 0
    try:
        return round(int(raw_value) / 1_000_000_000, 8)
    except (TypeError, ValueError):
        return 0.0



def transaction_is_incoming(tx: dict[str, Any]) -> bool:
    in_msg = tx.get("in_msg") or {}
    source = in_msg.get("source")
    return bool(source)


async def process_deposit_tx(application: Application, tx_hash: str, user_id: int, amount: float) -> None:
    user = get_user(user_id)
    if not user:
        logger.info("Deposit memo for unknown user %s", user_id)
        create_transaction(tx_hash, user_id, amount, "deposit", "failed")
        mark_processed_tx(tx_hash)
        return
    if amount + 1e-9 < MIN_DEPOSIT:
        create_transaction(tx_hash, user_id, amount, "deposit", "failed")
        mark_processed_tx(tx_hash)
        await safe_send(application.bot, user_id, f"⚠️ Deposit received but below minimum ({format_ton(MIN_DEPOSIT)} TON). It was not credited.")
        return
    adjust_user_balances(user_id, wallet_delta=amount)
    create_transaction(tx_hash, user_id, amount, "deposit", "confirmed")
    mark_processed_tx(tx_hash)
    await safe_send(application.bot, user_id, f"✅ Deposit confirmed! +{format_ton(amount)} TON credited to your wallet.")
    logger.info("Deposit credited: user=%s amount=%s tx=%s", user_id, amount, tx_hash)


async def process_match_payment_tx(application: Application, tx_hash: str, match_id: int, amount: float) -> None:
    match = get_match(match_id)
    if not match or match["status"] != "pending_payment":
        create_transaction(tx_hash, match["player1"] if match else None, amount, "match_entry", "failed")
        mark_processed_tx(tx_hash)
        return
    expected = float(match["entry_fee"] or 0)
    if amount + 1e-9 < expected:
        create_transaction(tx_hash, match["player1"], amount, "match_entry", "failed")
        mark_processed_tx(tx_hash)
        await safe_send(application.bot, match["player1"], f"⚠️ Match payment for #{match_id} was below the required {format_ton(expected)} TON.")
        return

    set_match_status(
        match_id,
        "waiting",
        locked_amount=expected,
        player1_pay_mode="external",
        player1_paid=expected,
    )
    create_transaction(tx_hash, match["player1"], amount, "match_entry", "confirmed")
    mark_processed_tx(tx_hash)
    await safe_send(application.bot, match["player1"], f"✅ Match payment confirmed for challenge #{match_id}. Your challenge is now live.")
    await post_waiting_challenge(application.bot, match_id)
    logger.info("Match payment confirmed: match=%s amount=%s tx=%s", match_id, amount, tx_hash)


# 3. Verification system handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sync_user_from_update(update)
    user = get_user(update.effective_user.id)
    if user and int(user["is_verified"] or 0) == -1:
        await safe_reply(update, "⛔ You have been banned. Contact admin.")
        return
    await safe_reply(update, "Welcome to PvP Bot 🎮")


async def verify_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_private(update):
        return
    user = await get_current_user(update)
    status = int(user["is_verified"] or 0)
    if status == -1:
        await safe_reply(update, "⛔ You have been banned. Contact admin.")
        return
    if status == 1:
        await safe_reply(update, "✅ You are already verified.")
        return
    if int(user["verification_requested"] or 0) == 1:
        await safe_reply(update, "⏳ Your verification request is already pending admin approval.")
        return

    update_user_verification(user["user_id"], 0, 1)
    label = username_label(user["username"], user["user_id"])
    await notify_admin(
        context.bot,
        "🔔 New verification request!\n"
        f"User: {label} (ID: {user['user_id']})\n"
        f"Use /approve {user['user_id']} or /reject {user['user_id']}",
    )
    await safe_reply(update, "✅ Verification request sent to admin.")


async def approve_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await safe_reply(update, "Usage: /approve <user_id>")
        return
    user_id = int(context.args[0])
    user = get_user(user_id)
    if not user:
        await safe_reply(update, "User not found.")
        return
    update_user_verification(user_id, 1, 0)
    await safe_send(context.bot, user_id, "✅ You have been verified and can now use paid matches and wallet features.")
    await safe_reply(update, f"Approved {username_label(user['username'], user_id)}")


async def reject_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    if not context.args or not context.args[0].isdigit():
        await safe_reply(update, "Usage: /reject <user_id> [reason]")
        return
    user_id = int(context.args[0])
    reason = " ".join(context.args[1:]).strip()
    user = get_user(user_id)
    if not user:
        await safe_reply(update, "User not found.")
        return
    update_user_verification(user_id, 0, 0)
    message = "❌ Your verification request was rejected."
    if reason:
        message += f"\nReason: {reason}"
    await safe_send(context.bot, user_id, message)
    await safe_reply(update, f"Rejected {username_label(user['username'], user_id)}")


# 4. Wallet handlers (/deposit, /withdraw, /balance)
async def deposit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_private(update):
        return
    user = await require_verified_user(update)
    if user is None:
        return
    if not PLATFORM_TON_WALLET:
        await safe_reply(update, "⚠️ Platform wallet is not configured. Contact admin.")
        return
    await safe_reply(
        update,
        f"Send TON to: {PLATFORM_TON_WALLET}\n"
        f"Memo/Tag: {user['user_id']}\n"
        f"Min deposit: {format_ton(MIN_DEPOSIT)} TON\n"
        "Your balance will update within 2 minutes.",
    )


async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_private(update):
        return
    user = await require_verified_user(update)
    if user is None:
        return
    if len(context.args) != 2:
        await safe_reply(update, "Usage: /withdraw <amount> <ton_address>")
        return
    try:
        amount = parse_ton_amount(context.args[0])
    except ValueError as exc:
        await safe_reply(update, str(exc))
        return
    if amount + 1e-9 < MIN_WITHDRAWAL:
        await safe_reply(update, f"Minimum withdrawal is {format_ton(MIN_WITHDRAWAL)} TON.")
        return
    ton_address = context.args[1].strip()
    try:
        Address(ton_address)
    except Exception:
        await safe_reply(update, "Invalid TON address.")
        return
    available = float(user["wallet_balance"] or 0)
    if available + 1e-9 < amount:
        await safe_reply(update, "⚠️ Insufficient wallet balance.")
        return

    tx_ref = f"withdraw:pending:{uuid.uuid4().hex}"
    create_transaction(tx_ref, user["user_id"], -amount, "withdraw", "pending")
    try:
        tx_hash = await send_ton_withdrawal(amount, ton_address, f"withdraw:{user['user_id']}")
        adjust_user_balances(user["user_id"], wallet_delta=-amount)
        set_user_ton_address(user["user_id"], ton_address)
        create_transaction(tx_hash, user["user_id"], -amount, "withdraw", "confirmed")
        await safe_reply(update, f"✅ Withdrawal of {format_ton(amount)} TON sent to {ton_address}")
        await safe_send(context.bot, user["user_id"], f"✅ Withdrawal of {format_ton(amount)} TON sent to {ton_address}")
        logger.info("Withdrawal sent: user=%s amount=%s address=%s", user["user_id"], amount, ton_address)
    except Exception as exc:
        logger.exception("Withdrawal failed: %s", exc)
        create_transaction(tx_ref, user["user_id"], -amount, "withdraw", "failed")
        await safe_reply(update, "⚠️ Withdrawal failed. Try again or contact admin.")


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_private(update):
        return
    user = await get_current_user(update)
    total = float(user["wallet_balance"] or 0) + float(user["locked_balance"] or 0)
    locked = float(user["locked_balance"] or 0)
    available = float(user["wallet_balance"] or 0)
    await safe_reply(
        update,
        "💰 Your Wallet\n"
        f"TON Balance: {format_ton(total)} TON\n"
        f"Locked (in match): {format_ton(locked)} TON\n"
        f"Available: {format_ton(available)} TON",
    )


# 5. Match system handlers (/challenge, /accept, /result, /resolve)
async def setmlbb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_private(update):
        return
    user = await get_current_user(update)
    if int(user["is_verified"] or 0) == -1:
        await safe_reply(update, "⛔ You have been banned. Contact admin.")
        return
    if len(context.args) != 1:
        await safe_reply(update, "Usage: /setmlbb <mlbb_id>")
        return
    mlbb_id = context.args[0].strip()
    if not mlbb_id:
        await safe_reply(update, "Usage: /setmlbb <mlbb_id>")
        return
    set_user_mlbb(user["user_id"], mlbb_id)
    await safe_reply(update, "MLBB ID saved ✅")


async def challenge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_group(update):
        return
    user = await require_verified_user(update)
    if user is None:
        return
    if len(context.args) not in {2, 3}:
        await safe_reply(update, "Usage: /challenge <amount> <game> [--pay]")
        return
    pay_mode = len(context.args) == 3 and context.args[2].lower() == "--pay"
    if len(context.args) == 3 and not pay_mode:
        await safe_reply(update, "Usage: /challenge <amount> <game> [--pay]")
        return

    try:
        entry_fee = parse_ton_amount(context.args[0])
    except ValueError as exc:
        await safe_reply(update, str(exc))
        return
    if entry_fee + 1e-9 < MIN_ENTRY_FEE:
        await safe_reply(update, f"Minimum entry fee is {format_ton(MIN_ENTRY_FEE)} TON.")
        return

    game = context.args[1].strip().lower()
    if game not in {"dice", "chess", "mlbb"}:
        await safe_reply(update, "Game must be one of: dice, chess, mlbb")
        return
    if game == "mlbb" and not user["mlbb_id"]:
        await safe_reply(update, "⚠️ Set your MLBB ID first with /setmlbb in private chat.")
        return
    if pay_mode and not PLATFORM_TON_WALLET:
        await safe_reply(update, "⚠️ Platform wallet is not configured. Contact admin.")
        return

    status = "pending_payment" if pay_mode else "waiting"
    with closing(get_conn()) as conn, conn:
        cursor = conn.execute(
            """
            INSERT INTO matches (
                player1, game, amount, entry_fee, locked_amount, status,
                created_at, group_chat_id, player1_pay_mode, player1_paid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user["user_id"],
                game,
                format_ton(entry_fee),
                entry_fee,
                0.0 if pay_mode else entry_fee,
                status,
                utc_now_str(),
                update.effective_chat.id,
                "external" if pay_mode else "wallet",
                0.0 if pay_mode else entry_fee,
            ),
        )
        match_id = int(cursor.lastrowid)

    if pay_mode:
        await safe_reply(
            update,
            f"Send {format_ton(entry_fee)} TON to: {PLATFORM_TON_WALLET}\n"
            f"Memo/Tag: {match_id}\n"
            "This challenge will activate after payment is confirmed.\n"
            f"Payment window: {MATCH_PAYMENT_WINDOW_MINUTES} minutes.",
        )
        return

    try:
        lock_wallet_entry(user["user_id"], entry_fee, str(match_id))
    except ValueError:
        set_match_status(match_id, "cancelled", locked_amount=0, player1_paid=0)
        await safe_reply(update, "⚠️ Insufficient wallet balance.")
        return

    set_match_status(match_id, "waiting", locked_amount=entry_fee, player1_paid=entry_fee)
    message_id = await post_waiting_challenge(context.bot, match_id)
    if message_id is None:
        await safe_reply(update, f"Challenge #{match_id} created, but I could not post it automatically.")


async def accept_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_group(update):
        return
    user = await require_verified_user(update)
    if user is None:
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await safe_reply(update, "Usage: /accept <match_id>")
        return

    match = get_match(int(context.args[0]))
    if not match:
        await safe_reply(update, "Invalid match ID.")
        return
    if match["status"] != "waiting":
        await safe_reply(update, "This match is not available for acceptance.")
        return
    if match["player1"] == user["user_id"]:
        await safe_reply(update, "You cannot accept your own challenge.")
        return
    if match["group_chat_id"] != update.effective_chat.id:
        await safe_reply(update, "This match belongs to another group.")
        return
    if match["game"] == "mlbb" and not user["mlbb_id"]:
        await safe_reply(update, "⚠️ Set your MLBB ID first with /setmlbb in private chat.")
        return

    entry_fee = float(match["entry_fee"] or 0)
    if float(user["wallet_balance"] or 0) + 1e-9 < entry_fee:
        await safe_reply(update, "⚠️ Insufficient wallet balance.")
        return

    try:
        lock_wallet_entry(user["user_id"], entry_fee, str(match["match_id"]))
    except ValueError:
        await safe_reply(update, "⚠️ Insufficient wallet balance.")
        return

    with closing(get_conn()) as conn, conn:
        conn.execute(
            """
            UPDATE matches
            SET player2 = ?, player2_pay_mode = 'wallet', player2_paid = ?,
                locked_amount = ?, status = 'active', started_at = ?
            WHERE match_id = ?
            """,
            (
                user["user_id"],
                entry_fee,
                round(float(match["locked_amount"] or 0) + entry_fee, 8),
                utc_now_str(),
                match["match_id"],
            ),
        )

    player1 = get_user(match["player1"])
    player2 = get_user(user["user_id"])
    if not player1 or not player2:
        await safe_reply(update, "User data missing.")
        return
    active_match = get_match(match["match_id"])
    gross = entry_fee * 2
    net = gross * (1 - PLATFORM_FEE_RATE)

    if active_match["game"] == "dice":
        import random

        while True:
            roll1 = random.randint(1, 6)
            roll2 = random.randint(1, 6)
            if roll1 != roll2:
                break
        winner_id = player1["user_id"] if roll1 > roll2 else player2["user_id"]
        winner_label = username_label(player1["username"], player1["user_id"]) if winner_id == player1["user_id"] else username_label(player2["username"], player2["user_id"])
        match_after, payout = finalize_match_payout(active_match["match_id"], winner_id)
        await safe_reply(
            update,
            "🎲 Dice Match Started!\n\n"
            f"{username_label(player1['username'], player1['user_id'])} rolled: 🎲 {roll1}\n"
            f"{username_label(player2['username'], player2['user_id'])} rolled: 🎲 {roll2}\n\n"
            f"🏆 Winner: {winner_label}\nPrize: {format_ton(payout)} TON credited to wallet",
        )
        await safe_send(context.bot, winner_id, f"✅ Payout sent! {format_ton(payout)} TON has been credited to your wallet for match #{match_after['match_id']}.")
        return

    if active_match["game"] == "chess":
        await safe_reply(
            update,
            "♟️ Chess Match Confirmed!\n\n"
            f"⚔️ Player 1: {username_label(player1['username'], player1['user_id'])}\n"
            f"⚔️ Player 2: {username_label(player2['username'], player2['user_id'])}\n\n"
            f"💰 Prize Pool: {format_ton(net)} TON\n"
            f"🏆 Winner takes all (minus {int(PLATFORM_FEE_RATE * 100)}% platform fee)\n\n"
            "Both players must submit:\n/result win  or  /result lose\n\n"
            f"Match ID: #{active_match['match_id']}\n"
            f"⏱️ Result deadline: {MATCH_RESULT_REMINDER_MINUTES} minutes",
        )
        return

    await safe_reply(
        update,
        "🎮 MLBB Match Confirmed!\n\n"
        f"⚔️ Player 1: {username_label(player1['username'], player1['user_id'])}\n"
        f"   MLBB ID: {player1['mlbb_id']}\n"
        f"⚔️ Player 2: {username_label(player2['username'], player2['user_id'])}\n"
        f"   MLBB ID: {player2['mlbb_id']}\n\n"
        f"💰 Prize Pool: {format_ton(net)} TON\n"
        f"🏆 Winner takes all (minus {int(PLATFORM_FEE_RATE * 100)}% platform fee)\n\n"
        "👉 Add each other in-game and start the match!\n"
        "👉 After match, BOTH players submit:\n"
        "/result win  or  /result lose\n\n"
        f"Match ID: #{active_match['match_id']}\n"
        f"⏱️ Result deadline: {MATCH_RESULT_REMINDER_MINUTES} minutes",
    )


async def mark_dispute(application: Application, match: sqlite3.Row, group_message: str) -> None:
    if match["status"] != "dispute":
        set_match_status(match["match_id"], "dispute")
        increment_dispute_stats(match)
    refreshed = get_match(match["match_id"])
    player1 = get_user(match["player1"])
    player2 = get_user(match["player2"]) if match["player2"] else None
    admin_message = (
        f"⚠️ Dispute in Match #{match['match_id']}!\n"
        f"Player1: {username_label(player1['username'], player1['user_id'])} → claimed {refreshed['result1'] or 'none'}\n"
        f"Player2: {username_label(player2['username'], player2['user_id']) if player2 else 'N/A'} → claimed {refreshed['result2'] or 'none'}\n"
        f"Use /resolve {match['match_id']} <winner_user_id>"
    )
    await notify_admin(application.bot, admin_message)
    await safe_send(application.bot, match["group_chat_id"], group_message)
    if player1:
        await safe_send(application.bot, player1["user_id"], f"⚠️ Match #{match['match_id']} is in dispute. Admin has been notified.")
    if player2:
        await safe_send(application.bot, player2["user_id"], f"⚠️ Match #{match['match_id']} is in dispute. Admin has been notified.")


async def result_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_group(update):
        return
    user = await get_current_user(update)
    if int(user["is_verified"] or 0) == -1:
        await safe_reply(update, "⛔ You have been banned. Contact admin.")
        return
    if len(context.args) != 1 or context.args[0].lower() not in {"win", "lose"}:
        await safe_reply(update, "Usage: /result <win/lose>")
        return
    result = context.args[0].lower()

    matches = get_active_manual_matches(user["user_id"])
    if not matches:
        await safe_reply(update, "No active manual match found for you.")
        return
    if len(matches) > 1 and CHESS_RESULT_SELECTION != "latest":
        await safe_reply(update, "You have multiple active manual matches. Resolve older matches first.")
        return
    match = matches[0]
    store_match_result(match["match_id"], user["user_id"], result)
    updated = get_match(match["match_id"])
    if not updated:
        await safe_reply(update, "Match not found.")
        return

    if not updated["result1"] or not updated["result2"]:
        await safe_reply(update, "Result submitted. Waiting for the other player.")
        return

    if updated["result1"] == updated["result2"]:
        await mark_dispute(
            context.application,
            updated,
            f"⚠️ Result disputed! Admin has been notified.\nPlease wait for admin decision.\nMatch ID: #{updated['match_id']}",
        )
        return

    winner_id = updated["player1"] if updated["result1"] == "win" else updated["player2"]
    match_after, payout = finalize_match_payout(updated["match_id"], winner_id)
    winner = get_user(winner_id)
    await safe_send(
        context.bot,
        match_after["group_chat_id"],
        "🏆 Match #{} Result!\n\nWinner: {} (verified by both players)\nPrize: {} TON credited to wallet\n\nGG WP! 🎉".format(
            match_after["match_id"],
            username_label(winner["username"], winner_id) if winner else f"User {winner_id}",
            format_ton(payout),
        ),
    )
    await safe_send(context.bot, winner_id, f"✅ Payout sent! {format_ton(payout)} TON has been credited to your wallet.")


async def resolve_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    if len(context.args) != 2 or not context.args[0].isdigit() or not context.args[1].isdigit():
        await safe_reply(update, "Usage: /resolve <match_id> <winner_user_id>")
        return
    match_id = int(context.args[0])
    winner_id = int(context.args[1])
    match = get_match(match_id)
    if not match:
        await safe_reply(update, "Match not found.")
        return
    if winner_id not in {match["player1"], match["player2"]}:
        await safe_reply(update, "Winner must be one of the match players.")
        return
    match_after, payout = finalize_match_payout(match_id, winner_id)
    winner = get_user(winner_id)
    await safe_send(
        context.bot,
        match_after["group_chat_id"],
        f"🏆 Match #{match_after['match_id']} Result!\n\nWinner: {username_label(winner['username'], winner_id) if winner else winner_id}\nPrize: {format_ton(payout)} TON credited to wallet\n\nAdmin resolved the result.",
    )
    await safe_send(context.bot, winner_id, f"✅ Payout sent! {format_ton(payout)} TON has been credited to your wallet.")
    await safe_reply(update, f"Match #{match_id} resolved.")


# 6. Profile handler (/profile)
async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_private(update):
        return
    user = await get_current_user(update)
    stats = get_user_match_stats(user["user_id"])
    wins = int(user["wins"] or 0)
    losses = int(user["losses"] or 0)
    disputes = int(user["disputes"] or 0)
    decided = wins + losses
    win_rate = (wins / decided * 100) if decided else 0.0
    total_balance = float(user["wallet_balance"] or 0) + float(user["locked_balance"] or 0)
    await safe_reply(
        update,
        "👤 Your Profile\n\n"
        f"Username: {username_label(user['username'], user['user_id'])}\n"
        f"MLBB ID: {user['mlbb_id'] or 'Not set'}\n"
        f"Status: {verification_status_text(user)}\n\n"
        "💰 Wallet\n"
        f"TON Balance: {format_ton(total_balance)} TON\n"
        f"Locked: {format_ton(float(user['locked_balance'] or 0))} TON\n\n"
        "🎮 Match Stats\n"
        f"Total Matches: {int(stats['total_matches'])}\n"
        f"Wins: {wins} | Losses: {losses} | Disputes: {disputes}\n"
        f"Win Rate: {win_rate:.0f}%\n\n"
        "🏆 Earnings\n"
        f"Total Won: {format_ton(float(user['total_earned'] or 0))} TON\n"
        f"Total Lost: {format_ton(float(stats['total_lost']))} TON",
    )


# 7. Admin dashboard handlers
async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    with closing(get_conn()) as conn:
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        verified_users = conn.execute("SELECT COUNT(*) FROM users WHERE is_verified = 1").fetchone()[0]
        total_matches = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        completed = conn.execute("SELECT COUNT(*) FROM matches WHERE status = 'completed'").fetchone()[0]
        disputed = conn.execute("SELECT COUNT(*) FROM matches WHERE status = 'dispute'").fetchone()[0]
        active = conn.execute("SELECT COUNT(*) FROM matches WHERE status = 'active'").fetchone()[0]
        volume = float(
            conn.execute("SELECT COALESCE(SUM(entry_fee * 2), 0) FROM matches WHERE payout_sent = 1 AND winner_id IS NOT NULL").fetchone()[0]
            or 0
        )
        earnings = float(
            conn.execute(
                "SELECT COALESCE(SUM((entry_fee * 2) - ((entry_fee * 2) * ?)), 0) FROM matches WHERE payout_sent = 1 AND winner_id IS NOT NULL",
                (1 - PLATFORM_FEE_RATE,),
            ).fetchone()[0]
            or 0
        )
    await safe_reply(
        update,
        "Admin Stats\n\n"
        f"Total Users: {total_users} | Verified Users: {verified_users}\n"
        f"Total Matches: {total_matches} | Completed: {completed} | Disputed: {disputed} | Active: {active}\n"
        f"Platform Earnings: {format_ton(earnings)} TON\n"
        f"Total TON Volume: {format_ton(volume)} TON",
    )


async def admin_matches_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT m.*, u1.username AS p1_username, u2.username AS p2_username
            FROM matches m
            LEFT JOIN users u1 ON u1.user_id = m.player1
            LEFT JOIN users u2 ON u2.user_id = m.player2
            ORDER BY m.match_id DESC
            LIMIT 10
            """
        ).fetchall()
    if not rows:
        await safe_reply(update, "No matches found.")
        return
    lines = ["Last 10 Matches"]
    for row in rows:
        lines.append(
            f"#{row['match_id']} | {username_label(row['p1_username'], row['player1'])} vs {username_label(row['p2_username'], row['player2']) if row['player2'] else '—'} | {row['game']} | {format_ton(float(row['entry_fee'] or 0))} TON | {row['status']}"
        )
    await safe_reply(update, "\n".join(lines))


async def admin_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await safe_reply(update, "Usage: /admin_user <user_id>")
        return
    user = get_user(int(context.args[0]))
    if not user:
        await safe_reply(update, "User not found.")
        return
    stats = get_user_match_stats(user["user_id"])
    recent = get_recent_matches_for_user(user["user_id"], 5)
    lines = [
        f"User: {username_label(user['username'], user['user_id'])}",
        f"ID: {user['user_id']}",
        f"MLBB ID: {user['mlbb_id'] or 'Not set'}",
        f"Status: {verification_status_text(user)}",
        f"Wallet: {format_ton(float(user['wallet_balance'] or 0))} TON",
        f"Locked: {format_ton(float(user['locked_balance'] or 0))} TON",
        f"Wins: {user['wins']} | Losses: {user['losses']} | Disputes: {user['disputes']}",
        f"Total Earned: {format_ton(float(user['total_earned'] or 0))} TON",
        f"Total Lost: {format_ton(float(stats['total_lost']))} TON",
        "Recent Matches:",
    ]
    if recent:
        for match in recent:
            lines.append(
                f"#{match['match_id']} {match['game']} {format_ton(float(match['entry_fee'] or 0))} TON {match['status']}"
            )
    else:
        lines.append("No match history.")
    await safe_reply(update, "\n".join(lines))


async def admin_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    try:
        balance = await fetch_platform_wallet_balance()
        await safe_reply(update, f"Platform wallet balance: {format_ton(balance)} TON")
    except Exception as exc:
        logger.exception("Failed to fetch platform balance: %s", exc)
        await safe_reply(update, "Failed to fetch platform wallet balance.")


async def admin_refund_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await safe_reply(update, "Usage: /admin_refund <match_id>")
        return
    match_id = int(context.args[0])
    try:
        match = refund_match(match_id)
    except ValueError as exc:
        await safe_reply(update, str(exc))
        return
    await safe_reply(update, f"Refunded match #{match_id}.")
    for user_id in [match["player1"], match["player2"]]:
        if user_id:
            await safe_send(context.bot, user_id, f"Refund processed for match #{match_id}. Entry fee returned to your wallet.")
    if match["group_chat_id"]:
        await safe_send(context.bot, match["group_chat_id"], f"Admin refunded match #{match_id}. Both players have been refunded.")


async def admin_ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await safe_reply(update, "Usage: /admin_ban <user_id>")
        return
    user_id = int(context.args[0])
    user = get_user(user_id)
    if not user:
        await safe_reply(update, "User not found.")
        return
    update_user_verification(user_id, -1, 0)
    await safe_send(context.bot, user_id, "You have been banned. Contact admin.")
    await safe_reply(update, f"Banned {username_label(user['username'], user_id)}")


async def admin_unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await safe_reply(update, "Usage: /admin_unban <user_id>")
        return
    user_id = int(context.args[0])
    user = get_user(user_id)
    if not user:
        await safe_reply(update, "User not found.")
        return
    update_user_verification(user_id, 0, 0)
    await safe_send(context.bot, user_id, "✅ Your account has been restored. You can request verification again with /verify.")
    await safe_reply(update, f"Unbanned {username_label(user['username'], user_id)}")


# 8. Background job functions
async def deposit_and_payment_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    application = context.application
    with closing(get_conn()) as conn, conn:
        expired = conn.execute(
            "SELECT * FROM matches WHERE status = 'pending_payment' AND created_at <= ?",
            ((utc_now() - timedelta(minutes=MATCH_PAYMENT_WINDOW_MINUTES)).strftime("%Y-%m-%d %H:%M:%S"),),
        ).fetchall()
        for match in expired:
            conn.execute("UPDATE matches SET status = 'cancelled' WHERE match_id = ?", (match["match_id"],))
            await safe_send(application.bot, match["player1"], f"⚠️ Payment window expired for challenge #{match['match_id']}. The challenge was cancelled.")
            if match["group_chat_id"]:
                await safe_send(application.bot, match["group_chat_id"], f"Challenge #{match['match_id']} expired because payment was not received in time.")

    if not PLATFORM_TON_WALLET:
        return

    try:
        transactions = await fetch_platform_transactions()
    except Exception as exc:
        logger.exception("Failed to poll TonCenter transactions: %s", exc)
        return

    for tx in transactions:
        tx_hash = extract_tx_hash(tx)
        if not tx_hash or processed_tx_exists(tx_hash):
            continue
        if not transaction_is_incoming(tx):
            mark_processed_tx(tx_hash)
            continue
        memo = extract_tx_comment(tx)
        amount = extract_tx_amount(tx)
        if not memo:
            mark_processed_tx(tx_hash)
            continue
        if memo.isdigit() and get_match(int(memo)) and get_match(int(memo))["status"] == "pending_payment":
            await process_match_payment_tx(application, tx_hash, int(memo), amount)
            continue
        if memo.isdigit() and get_user(int(memo)):
            await process_deposit_tx(application, tx_hash, int(memo), amount)
            continue
        mark_processed_tx(tx_hash)


async def match_timeout_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    application = context.application
    reminded = application.bot_data.setdefault("reminded_matches", set())
    dispute_cutoff = utc_now() - timedelta(minutes=MATCH_RESULT_DISPUTE_MINUTES)
    reminder_cutoff = utc_now() - timedelta(minutes=MATCH_RESULT_REMINDER_MINUTES)
    with closing(get_conn()) as conn:
        active_matches = conn.execute(
            "SELECT * FROM matches WHERE status = 'active' AND game IN ('mlbb', 'chess')"
        ).fetchall()

    for match in active_matches:
        started_at = parse_db_time(match["started_at"] or match["created_at"])
        if not started_at:
            continue
        if started_at <= dispute_cutoff and (not match["result1"] or not match["result2"]):
            await mark_dispute(
                application,
                match,
                f"⚠️ Result disputed! Admin has been notified.\nPlease wait for admin decision.\nMatch ID: #{match['match_id']}",
            )
            reminded.discard(match["match_id"])
            continue
        if started_at <= reminder_cutoff and match["match_id"] not in reminded and (not match["result1"] or not match["result2"]):
            text = (
                f"⏰ Reminder for match #{match['match_id']}: both players must submit results with /result win or /result lose."
            )
            await safe_send(application.bot, match["group_chat_id"], text)
            if match["player1"]:
                await safe_send(application.bot, match["player1"], text)
            if match["player2"]:
                await safe_send(application.bot, match["player2"], text)
            reminded.add(match["match_id"])


# 9. main() with ApplicationBuilder setup
async def post_init(application: Application) -> None:
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Existing webhook cleared before polling startup.")
    except TelegramError as exc:
        logger.warning("Failed to clear webhook before polling: %s", exc)


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing.")

    init_db()
    logger.info("Database initialized at %s", DB_PATH)

    asyncio.run(force_delete_webhook())

    application = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("setmlbb", setmlbb_command))
    application.add_handler(CommandHandler("verify", verify_command))
    application.add_handler(CommandHandler("approve", approve_command))
    application.add_handler(CommandHandler("reject", reject_command))
    application.add_handler(CommandHandler("deposit", deposit_command))
    application.add_handler(CommandHandler("withdraw", withdraw_command))
    application.add_handler(CommandHandler("balance", balance_command))
    application.add_handler(CommandHandler("challenge", challenge_command))
    application.add_handler(CommandHandler("accept", accept_command))
    application.add_handler(CommandHandler("result", result_command))
    application.add_handler(CommandHandler("resolve", resolve_command))
    application.add_handler(CommandHandler("profile", profile_command))
    application.add_handler(CommandHandler("admin_stats", admin_stats_command))
    application.add_handler(CommandHandler("admin_matches", admin_matches_command))
    application.add_handler(CommandHandler("admin_user", admin_user_command))
    application.add_handler(CommandHandler("admin_balance", admin_balance_command))
    application.add_handler(CommandHandler("admin_refund", admin_refund_command))
    application.add_handler(CommandHandler("admin_ban", admin_ban_command))
    application.add_handler(CommandHandler("admin_unban", admin_unban_command))

    if application.job_queue is None:
        raise RuntimeError("Job queue is unavailable. Install python-telegram-bot with job-queue extras.")

    application.job_queue.run_repeating(deposit_and_payment_job, interval=60, first=15, name="deposit_and_payment_job")
    application.job_queue.run_repeating(match_timeout_job, interval=300, first=60, name="match_timeout_job")

    logger.info("Bot starting...")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
