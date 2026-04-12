import os
import sqlite3
from contextlib import closing
from typing import Any, Optional

from .config import DB_PATH
from .utils import safe_username, utc_now_str


def get_conn() -> sqlite3.Connection:
    db_parent = os.path.dirname(DB_PATH)
    if db_parent:
        os.makedirs(db_parent, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


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

            CREATE INDEX IF NOT EXISTS idx_users_verified ON users(is_verified);
            CREATE INDEX IF NOT EXISTS idx_matches_status_game ON matches(status, game);
            CREATE INDEX IF NOT EXISTS idx_matches_group_chat ON matches(group_chat_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_user_type ON transactions(user_id, type);
            """
        )

        user_columns = {row[1] for row in conn.execute("PRAGMA table_info(users)")}
        for column, sql in [
            ("username", "ALTER TABLE users ADD COLUMN username TEXT"),
            ("wallet_balance", "ALTER TABLE users ADD COLUMN wallet_balance REAL DEFAULT 0"),
            ("locked_balance", "ALTER TABLE users ADD COLUMN locked_balance REAL DEFAULT 0"),
            ("ton_address", "ALTER TABLE users ADD COLUMN ton_address TEXT"),
            ("is_verified", "ALTER TABLE users ADD COLUMN is_verified INTEGER DEFAULT 0"),
            ("wins", "ALTER TABLE users ADD COLUMN wins INTEGER DEFAULT 0"),
            ("losses", "ALTER TABLE users ADD COLUMN losses INTEGER DEFAULT 0"),
            ("disputes", "ALTER TABLE users ADD COLUMN disputes INTEGER DEFAULT 0"),
            ("total_earned", "ALTER TABLE users ADD COLUMN total_earned REAL DEFAULT 0"),
            ("created_at", "ALTER TABLE users ADD COLUMN created_at TEXT DEFAULT (datetime('now'))"),
            ("verification_requested", "ALTER TABLE users ADD COLUMN verification_requested INTEGER DEFAULT 0"),
        ]:
            if column not in user_columns:
                conn.execute(sql)

        match_columns = {row[1] for row in conn.execute("PRAGMA table_info(matches)")}
        for column, sql in [
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
            if column not in match_columns:
                conn.execute(sql)


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


def sync_user_from_update(update) -> None:
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


def update_match_after_challenge(match_id: int, message_id: Optional[int]) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute("UPDATE matches SET challenge_message_id = ? WHERE match_id = ?", (message_id, match_id))


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
