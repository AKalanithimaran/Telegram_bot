from contextlib import closing
from datetime import timedelta

from telegram.ext import ContextTypes

from .config import MATCH_PAYMENT_WINDOW_MINUTES, MATCH_RESULT_DISPUTE_MINUTES, MATCH_RESULT_REMINDER_MINUTES, PLATFORM_TON_WALLET, logger
from .database import get_conn, get_match, get_user, mark_processed_tx, processed_tx_exists
from .match_service import challenge_post_text, mark_dispute
from .telegram_helpers import post_waiting_challenge, safe_send
from .ton import (
    extract_tx_amount,
    extract_tx_comment,
    extract_tx_hash,
    fetch_platform_transactions,
    process_deposit_tx,
    process_match_payment_tx,
    transaction_is_incoming,
)
from .utils import parse_db_time, utc_now


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
        match = get_match(int(memo)) if memo.isdigit() else None
        if match and match["status"] == "pending_payment":
            await process_match_payment_tx(application, tx_hash, int(memo), amount, lambda bot, match_id: post_waiting_challenge(bot, match_id, challenge_post_text))
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
        active_matches = conn.execute("SELECT * FROM matches WHERE status = 'active' AND game IN ('mlbb', 'chess')").fetchall()
    for match in active_matches:
        started_at = parse_db_time(match["started_at"] or match["created_at"])
        if not started_at:
            continue
        if started_at <= dispute_cutoff and (not match["result1"] or not match["result2"]):
            await mark_dispute(application, match, f"⚠️ Result disputed! Admin has been notified.\nPlease wait for admin decision.\nMatch ID: #{match['match_id']}")
            reminded.discard(match["match_id"])
            continue
        if started_at <= reminder_cutoff and match["match_id"] not in reminded and (not match["result1"] or not match["result2"]):
            text = f"⏰ Reminder for match #{match['match_id']}: both players must submit results with /result win or /result lose."
            await safe_send(application.bot, match["group_chat_id"], text)
            if match["player1"]:
                await safe_send(application.bot, match["player1"], text)
            if match["player2"]:
                await safe_send(application.bot, match["player2"], text)
            reminded.add(match["match_id"])
