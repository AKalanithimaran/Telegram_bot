from __future__ import annotations

from datetime import timedelta
from typing import Any

from telegram import InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import settings
from db.models import (
    add_transaction,
    create_match,
    get_match,
    get_settings_doc,
    get_user,
    increment_wager_stats,
    record_game_result,
    refund_balance,
    reserve_balance,
    update_match,
)
from services.house import add_house_fee
from utils import ANTI_CHEAT_WARNING, display_name, format_amount, utcnow

GAME_FEE_RATE = 0.05


def challenge_summary(match: dict[str, Any], challenger: dict[str, Any]) -> str:
    mode = match.get("mode", "normal")
    dice_count = int(match.get("dice_count", 1))
    return "\n".join(
        [
            "New challenge",
            f"Match ID: `{match['_id']}`",
            f"Player: {display_name(challenger)}",
            f"Game: {match['game']}",
            f"Mode: {mode}",
            f"Dice Count: {dice_count}",
            f"Wager: {format_amount(float(match['amount']))} TON",
            ANTI_CHEAT_WARNING,
        ]
    )


async def create_challenge(
    user: dict[str, Any],
    amount: float,
    game: str,
    mode: str,
    dice_count: int,
    chat_id: int,
) -> dict[str, Any]:
    if not await reserve_balance(user["_id"], amount):
        raise ValueError("Insufficient balance.")
    await increment_wager_stats(user["_id"], amount)
    await add_transaction(user["_id"], "game_loss", -amount, "pending", metadata={"stage": "escrow"})
    return await create_match(
        {
            "game": game,
            "mode": mode,
            "dice_count": dice_count,
            "challenger_id": str(user["_id"]),
            "opponent_id": None,
            "amount": float(amount),
            "status": "pending",
            "winner_id": None,
            "challenger_result": None,
            "opponent_result": None,
            "chat_id": str(chat_id),
        }
    )


async def cancel_match_and_refund(match: dict[str, Any]) -> None:
    amount = float(match["amount"])
    if match.get("challenger_id"):
        await refund_balance(match["challenger_id"], amount)
    if match.get("opponent_id"):
        await refund_balance(match["opponent_id"], amount)
    await update_match(
        match["_id"],
        {"status": "cancelled", "completed_at": utcnow()},
    )


async def settle_match(match: dict[str, Any], winner_id: str) -> tuple[dict[str, Any], float, float]:
    if match["status"] == "completed":
        return match, 0.0, 0.0
    amount = float(match["amount"])
    fee = round(amount * 2 * GAME_FEE_RATE, 8)
    payout = round(amount * 2 - fee, 8)
    loser_id = match["challenger_id"] if str(match["opponent_id"]) == str(winner_id) else match["opponent_id"]
    await record_game_result(winner_id, loser_id, amount, payout)
    await add_house_fee(fee)
    await add_transaction(winner_id, "game_win", payout, "completed", metadata={"match_id": match["_id"]})
    await add_transaction(loser_id, "game_loss", -amount, "completed", metadata={"match_id": match["_id"]})
    updated = await update_match(
        match["_id"],
        {"status": "completed", "winner_id": str(winner_id), "completed_at": utcnow()},
    )
    if not updated:
        raise RuntimeError("Match disappeared after settlement.")
    return updated, payout, fee


async def accept_challenge_and_activate(match: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    if not await reserve_balance(user["_id"], float(match["amount"])):
        raise ValueError("Insufficient balance.")
    await increment_wager_stats(user["_id"], float(match["amount"]))
    await add_transaction(user["_id"], "game_loss", -float(match["amount"]), "pending", metadata={"stage": "escrow"})
    updated = await update_match(
        match["_id"],
        {
            "opponent_id": str(user["_id"]),
            "status": "active",
        },
    )
    if not updated:
        raise RuntimeError("Failed to activate match.")
    return updated


async def run_dice_style_match(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    match: dict[str, Any],
    emoji: str,
) -> tuple[int, int]:
    dice_count = int(match.get("dice_count", 1))
    mode = match.get("mode", "normal")
    challenger_total = 0
    opponent_total = 0
    while True:
        challenger_total = 0
        opponent_total = 0
        for _ in range(dice_count):
            first_roll = await context.bot.send_dice(chat_id=update.effective_chat.id, emoji=emoji)
            second_roll = await context.bot.send_dice(chat_id=update.effective_chat.id, emoji=emoji)
            challenger_total += int(first_roll.dice.value)
            opponent_total += int(second_roll.dice.value)
        if challenger_total != opponent_total:
            break
        await update.effective_message.reply_text("Tie detected, re-rolling automatically.")
    if mode == "crazy":
        winner_id = match["challenger_id"] if challenger_total < opponent_total else match["opponent_id"]
    else:
        winner_id = match["challenger_id"] if challenger_total > opponent_total else match["opponent_id"]
    return challenger_total, opponent_total if str(winner_id) == str(match["opponent_id"]) else opponent_total


async def expire_old_games(application) -> None:
    from db.models import fetch_pending_chess_matches

    timeout_before = utcnow() - timedelta(hours=2)
    matches = await fetch_pending_chess_matches(timeout_before)
    for match in matches:
        await cancel_match_and_refund(match)
        message = f"Chess match `{match['_id']}` expired after 2 hours and both players were refunded."
        for chat_id in {match.get("chat_id"), match.get("challenger_id"), match.get("opponent_id")}:
            if chat_id:
                await application.bot.send_message(chat_id=int(chat_id), text=message)
