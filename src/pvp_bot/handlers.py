import random
import uuid
from contextlib import closing

from telegram import Update
from telegram.ext import Application, ContextTypes
from tonsdk.utils import Address

from .config import (
    ADMIN_ID,
    MATCH_PAYMENT_WINDOW_MINUTES,
    MATCH_RESULT_REMINDER_MINUTES,
    MIN_DEPOSIT,
    MIN_ENTRY_FEE,
    MIN_WITHDRAWAL,
    PLATFORM_FEE_RATE,
    PLATFORM_TON_WALLET,
    logger,
)
from .database import (
    create_transaction,
    get_active_manual_matches,
    get_conn,
    get_match,
    get_recent_matches_for_user,
    get_user,
    get_user_match_stats,
    lock_wallet_entry,
    set_match_status,
    set_user_mlbb,
    set_user_ton_address,
    store_match_result,
    update_user_verification,
)
from .match_service import (
    can_use_paid_features,
    challenge_post_text,
    finalize_match_payout,
    mark_dispute,
    prize_pool_text,
    refund_match,
    verification_status_text,
)
from .telegram_helpers import (
    get_current_user,
    notify_admin,
    post_waiting_challenge,
    require_admin_private,
    require_group,
    require_private,
    safe_reply,
    safe_send,
)
from .ton import fetch_platform_wallet_balance, send_ton_withdrawal
from .utils import format_ton, parse_ton_amount, username_label


async def require_verified_user(update: Update):
    user = await get_current_user(update)
    allowed, message = can_use_paid_features(user)
    if not allowed:
        await safe_reply(update, message)
        return None
    return user


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await get_current_user(update)
    if int(user["is_verified"] or 0) == -1:
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
    await notify_admin(context.bot, f"🔔 New verification request!\nUser: {label} (ID: {user['user_id']})\nUse /approve {user['user_id']} or /reject {user['user_id']}")
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


async def deposit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_private(update):
        return
    user = await require_verified_user(update)
    if user is None:
        return
    if not PLATFORM_TON_WALLET:
        await safe_reply(update, "⚠️ Platform wallet is not configured. Contact admin.")
        return
    await safe_reply(update, f"Send TON to: {PLATFORM_TON_WALLET}\nMemo/Tag: {user['user_id']}\nMin deposit: {format_ton(MIN_DEPOSIT)} TON\nYour balance will update within 2 minutes.")


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
    if float(user["wallet_balance"] or 0) + 1e-9 < amount:
        await safe_reply(update, "⚠️ Insufficient wallet balance.")
        return
    tx_ref = f"withdraw:pending:{uuid.uuid4().hex}"
    create_transaction(tx_ref, user["user_id"], -amount, "withdraw", "pending")
    try:
        tx_hash = await send_ton_withdrawal(amount, ton_address, f"withdraw:{user['user_id']}")
        from .database import adjust_user_balances

        adjust_user_balances(user["user_id"], wallet_delta=-amount)
        set_user_ton_address(user["user_id"], ton_address)
        create_transaction(tx_hash, user["user_id"], -amount, "withdraw", "confirmed")
        await safe_reply(update, f"✅ Withdrawal of {format_ton(amount)} TON sent to {ton_address}")
        await safe_send(context.bot, user["user_id"], f"✅ Withdrawal of {format_ton(amount)} TON sent to {ton_address}")
    except Exception as exc:
        logger.exception("Withdrawal failed: %s", exc)
        create_transaction(tx_ref, user["user_id"], -amount, "withdraw", "failed")
        await safe_reply(update, "⚠️ Withdrawal failed. Try again or contact admin.")


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_private(update):
        return
    user = await get_current_user(update)
    total = float(user["wallet_balance"] or 0) + float(user["locked_balance"] or 0)
    await safe_reply(update, f"💰 Your Wallet\nTON Balance: {format_ton(total)} TON\nLocked (in match): {format_ton(float(user['locked_balance'] or 0))} TON\nAvailable: {format_ton(float(user['wallet_balance'] or 0))} TON")


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
            (user["user_id"], game, format_ton(entry_fee), entry_fee, 0.0 if pay_mode else entry_fee, status, __import__('pvp_bot.utils', fromlist=['utc_now_str']).utc_now_str(), update.effective_chat.id, "external" if pay_mode else "wallet", 0.0 if pay_mode else entry_fee),
        )
        match_id = int(cursor.lastrowid)
    if pay_mode:
        await safe_reply(update, f"Send {format_ton(entry_fee)} TON to: {PLATFORM_TON_WALLET}\nMemo/Tag: {match_id}\nThis challenge will activate after payment is confirmed.\nPayment window: {MATCH_PAYMENT_WINDOW_MINUTES} minutes.")
        return
    try:
        lock_wallet_entry(user["user_id"], entry_fee, str(match_id))
    except ValueError:
        set_match_status(match_id, "cancelled", locked_amount=0, player1_paid=0)
        await safe_reply(update, "⚠️ Insufficient wallet balance.")
        return
    set_match_status(match_id, "waiting", locked_amount=entry_fee, player1_paid=entry_fee)
    message_id = await post_waiting_challenge(context.bot, match_id, challenge_post_text)
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
    from .utils import utc_now_str
    with closing(get_conn()) as conn, conn:
        conn.execute(
            "UPDATE matches SET player2 = ?, player2_pay_mode = 'wallet', player2_paid = ?, locked_amount = ?, status = 'active', started_at = ? WHERE match_id = ?",
            (user["user_id"], entry_fee, round(float(match["locked_amount"] or 0) + entry_fee, 8), utc_now_str(), match["match_id"]),
        )
    player1 = get_user(match["player1"])
    player2 = get_user(user["user_id"])
    active_match = get_match(match["match_id"])
    if not player1 or not player2 or not active_match:
        await safe_reply(update, "User data missing.")
        return
    net = entry_fee * 2 * (1 - PLATFORM_FEE_RATE)
    if active_match["game"] == "dice":
        while True:
            roll1 = random.randint(1, 6)
            roll2 = random.randint(1, 6)
            if roll1 != roll2:
                break
        winner_id = player1["user_id"] if roll1 > roll2 else player2["user_id"]
        winner_label = username_label(player1["username"], player1["user_id"]) if winner_id == player1["user_id"] else username_label(player2["username"], player2["user_id"])
        match_after, payout = finalize_match_payout(active_match["match_id"], winner_id)
        await safe_reply(update, f"🎲 Dice Match Started!\n\n{username_label(player1['username'], player1['user_id'])} rolled: 🎲 {roll1}\n{username_label(player2['username'], player2['user_id'])} rolled: 🎲 {roll2}\n\n🏆 Winner: {winner_label}\nPrize: {format_ton(payout)} TON credited to wallet")
        await safe_send(context.bot, winner_id, f"✅ Payout sent! {format_ton(payout)} TON has been credited to your wallet for match #{match_after['match_id']}.")
        return
    if active_match["game"] == "chess":
        await safe_reply(update, f"♟️ Chess Match Confirmed!\n\n⚔️ Player 1: {username_label(player1['username'], player1['user_id'])}\n⚔️ Player 2: {username_label(player2['username'], player2['user_id'])}\n\n💰 Prize Pool: {format_ton(net)} TON\n🏆 Winner takes all (minus {int(PLATFORM_FEE_RATE * 100)}% platform fee)\n\nBoth players must submit:\n/result win  or  /result lose\n\nMatch ID: #{active_match['match_id']}\n⏱️ Result deadline: {MATCH_RESULT_REMINDER_MINUTES} minutes")
        return
    await safe_reply(update, f"🎮 MLBB Match Confirmed!\n\n⚔️ Player 1: {username_label(player1['username'], player1['user_id'])}\n   MLBB ID: {player1['mlbb_id']}\n⚔️ Player 2: {username_label(player2['username'], player2['user_id'])}\n   MLBB ID: {player2['mlbb_id']}\n\n💰 Prize Pool: {format_ton(net)} TON\n🏆 Winner takes all (minus {int(PLATFORM_FEE_RATE * 100)}% platform fee)\n\n👉 Add each other in-game and start the match!\n👉 After match, BOTH players submit:\n/result win  or  /result lose\n\nMatch ID: #{active_match['match_id']}\n⏱️ Result deadline: {MATCH_RESULT_REMINDER_MINUTES} minutes")


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
    matches = get_active_manual_matches(user["user_id"])
    if not matches:
        await safe_reply(update, "No active manual match found for you.")
        return
    match = matches[0]
    store_match_result(match["match_id"], user["user_id"], context.args[0].lower())
    updated = get_match(match["match_id"])
    if not updated:
        await safe_reply(update, "Match not found.")
        return
    if not updated["result1"] or not updated["result2"]:
        await safe_reply(update, "Result submitted. Waiting for the other player.")
        return
    if updated["result1"] == updated["result2"]:
        await mark_dispute(context.application, updated, f"⚠️ Result disputed! Admin has been notified.\nPlease wait for admin decision.\nMatch ID: #{updated['match_id']}")
        return
    winner_id = updated["player1"] if updated["result1"] == "win" else updated["player2"]
    match_after, payout = finalize_match_payout(updated["match_id"], winner_id)
    winner = get_user(winner_id)
    await safe_send(context.bot, match_after["group_chat_id"], "🏆 Match #{} Result!\n\nWinner: {} (verified by both players)\nPrize: {} TON credited to wallet\n\nGG WP! 🎉".format(match_after["match_id"], username_label(winner["username"], winner_id) if winner else f"User {winner_id}", format_ton(payout)))
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
    await safe_send(context.bot, match_after["group_chat_id"], f"🏆 Match #{match_after['match_id']} Result!\n\nWinner: {username_label(winner['username'], winner_id) if winner else winner_id}\nPrize: {format_ton(payout)} TON credited to wallet\n\nAdmin resolved the result.")
    await safe_send(context.bot, winner_id, f"✅ Payout sent! {format_ton(payout)} TON has been credited to your wallet.")
    await safe_reply(update, f"Match #{match_id} resolved.")


async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_private(update):
        return
    user = await get_current_user(update)
    stats = get_user_match_stats(user["user_id"])
    wins, losses, disputes = int(user["wins"] or 0), int(user["losses"] or 0), int(user["disputes"] or 0)
    decided = wins + losses
    win_rate = (wins / decided * 100) if decided else 0.0
    total_balance = float(user["wallet_balance"] or 0) + float(user["locked_balance"] or 0)
    await safe_reply(update, f"👤 Your Profile\n\nUsername: {username_label(user['username'], user['user_id'])}\nMLBB ID: {user['mlbb_id'] or 'Not set'}\nStatus: {verification_status_text(user)}\n\n💰 Wallet\nTON Balance: {format_ton(total_balance)} TON\nLocked: {format_ton(float(user['locked_balance'] or 0))} TON\n\n🎮 Match Stats\nTotal Matches: {int(stats['total_matches'])}\nWins: {wins} | Losses: {losses} | Disputes: {disputes}\nWin Rate: {win_rate:.0f}%\n\n🏆 Earnings\nTotal Won: {format_ton(float(user['total_earned'] or 0))} TON\nTotal Lost: {format_ton(float(stats['total_lost']))} TON")


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
        volume = float(conn.execute("SELECT COALESCE(SUM(entry_fee * 2), 0) FROM matches WHERE payout_sent = 1 AND winner_id IS NOT NULL").fetchone()[0] or 0)
        earnings = float(conn.execute("SELECT COALESCE(SUM((entry_fee * 2) * ?), 0) FROM matches WHERE payout_sent = 1 AND winner_id IS NOT NULL", (PLATFORM_FEE_RATE,)).fetchone()[0] or 0)
    await safe_reply(update, f"Admin Stats\n\nTotal Users: {total_users} | Verified Users: {verified_users}\nTotal Matches: {total_matches} | Completed: {completed} | Disputed: {disputed} | Active: {active}\nPlatform Earnings: {format_ton(earnings)} TON\nTotal TON Volume: {format_ton(volume)} TON")


async def admin_matches_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_private(update):
        return
    with closing(get_conn()) as conn:
        rows = conn.execute("""SELECT m.*, u1.username AS p1_username, u2.username AS p2_username FROM matches m LEFT JOIN users u1 ON u1.user_id = m.player1 LEFT JOIN users u2 ON u2.user_id = m.player2 ORDER BY m.match_id DESC LIMIT 10""").fetchall()
    if not rows:
        await safe_reply(update, "No matches found.")
        return
    await safe_reply(update, "\n".join(["Last 10 Matches"] + [f"#{row['match_id']} | {username_label(row['p1_username'], row['player1'])} vs {username_label(row['p2_username'], row['player2']) if row['player2'] else '—'} | {row['game']} | {format_ton(float(row['entry_fee'] or 0))} TON | {row['status']}" for row in rows]))


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
    lines = [f"User: {username_label(user['username'], user['user_id'])}", f"ID: {user['user_id']}", f"MLBB ID: {user['mlbb_id'] or 'Not set'}", f"Status: {verification_status_text(user)}", f"Wallet: {format_ton(float(user['wallet_balance'] or 0))} TON", f"Locked: {format_ton(float(user['locked_balance'] or 0))} TON", f"Wins: {user['wins']} | Losses: {user['losses']} | Disputes: {user['disputes']}", f"Total Earned: {format_ton(float(user['total_earned'] or 0))} TON", f"Total Lost: {format_ton(float(stats['total_lost']))} TON", "Recent Matches:"]
    lines.extend([f"#{m['match_id']} {m['game']} {format_ton(float(m['entry_fee'] or 0))} TON {m['status']}" for m in recent] or ["No match history."])
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
