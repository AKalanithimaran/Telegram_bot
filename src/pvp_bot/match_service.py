from contextlib import closing
from typing import Optional

from telegram.ext import Application

from .config import CHESS_RESULT_SELECTION, PLATFORM_FEE_RATE
from .database import (
    create_transaction,
    get_active_manual_matches,
    get_conn,
    get_match,
    get_user,
    increment_dispute_stats,
)
from .telegram_helpers import notify_admin, safe_send
from .utils import format_ton, username_label


def verification_status_text(user) -> str:
    if int(user["is_verified"] or 0) == 1:
        return "✅ Verified"
    if int(user["is_verified"] or 0) == -1:
        return "⛔ Banned"
    if int(user["verification_requested"] or 0) == 1:
        return "⏳ Pending"
    return "❌ Not Verified"


def can_use_paid_features(user) -> tuple[bool, str]:
    status = int(user["is_verified"] or 0)
    if status == -1:
        return False, "⛔ You have been banned. Contact admin."
    if status != 1:
        return False, "⚠️ You must be verified to use this feature.\nSend /verify to request approval from admin."
    return True, ""


def prize_pool_text(entry_fee: float) -> str:
    return f"{format_ton(entry_fee * 2 * (1 - PLATFORM_FEE_RATE))} TON"


def challenge_post_text(match, challenger) -> str:
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


def finalize_match_payout(match_id: int, winner_id: int) -> tuple[object, float]:
    with closing(get_conn()) as conn, conn:
        match = conn.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()
        if not match:
            raise ValueError("Match not found.")
        if match["payout_sent"]:
            return match, 0.0
        if winner_id not in {match["player1"], match["player2"]}:
            raise ValueError("Winner must be one of the match players.")
        entry_fee = float(match["entry_fee"] or 0)
        payout = round((entry_fee * 2) * (1 - PLATFORM_FEE_RATE), 8)

        player1_paid = float(match["player1_paid"] or 0)
        player2_paid = float(match["player2_paid"] or 0)
        if player1_paid > 0 and match["player1_pay_mode"] == "wallet":
            conn.execute("UPDATE users SET locked_balance = MAX(locked_balance - ?, 0) WHERE user_id = ?", (player1_paid, match["player1"]))
        if player2_paid > 0 and match["player2"] and match["player2_pay_mode"] == "wallet":
            conn.execute("UPDATE users SET locked_balance = MAX(locked_balance - ?, 0) WHERE user_id = ?", (player2_paid, match["player2"]))

        loser_id = match["player1"] if match["player2"] == winner_id else match["player2"]
        conn.execute(
            "UPDATE users SET wallet_balance = wallet_balance + ?, total_earned = total_earned + ?, wins = wins + 1 WHERE user_id = ?",
            (payout, payout, winner_id),
        )
        if loser_id:
            conn.execute("UPDATE users SET losses = losses + 1 WHERE user_id = ?", (loser_id,))

        conn.execute(
            "UPDATE matches SET status = 'completed', winner_id = ?, payout_sent = 1, locked_amount = 0 WHERE match_id = ?",
            (winner_id, match_id),
        )

    create_transaction(f"match_payout:{match_id}:{winner_id}", winner_id, payout, "match_payout", "confirmed")
    refreshed = get_match(match_id)
    if refreshed is None:
        raise ValueError("Match disappeared after payout.")
    return refreshed, payout


def refund_match(match_id: int):
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
            conn.execute("UPDATE users SET wallet_balance = ?, locked_balance = ? WHERE user_id = ?", (wallet_balance, locked_balance, user_id))
            create_transaction(f"match_payout:refund:{match_id}:{user_id}", user_id, amount, "match_payout", "confirmed")
        conn.execute("UPDATE matches SET status = 'cancelled', payout_sent = 1, locked_amount = 0 WHERE match_id = ?", (match_id,))
    refunded = get_match(match_id)
    if refunded is None:
        raise ValueError("Match disappeared after refund.")
    return refunded


def choose_manual_result_match(user_id: int):
    matches = get_active_manual_matches(user_id)
    if not matches:
        return None
    if len(matches) == 1 or CHESS_RESULT_SELECTION == "latest":
        return matches[0]
    return None


async def mark_dispute(application: Application, match, group_message: str) -> None:
    if match["status"] != "dispute":
        from .database import set_match_status

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
