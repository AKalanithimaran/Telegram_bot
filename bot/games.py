from __future__ import annotations

import random
from datetime import timedelta
from typing import Any

from telegram import Update
from telegram.constants import ChatType
from telegram.ext import ContextTypes
from pymongo import ReturnDocument

from bot.keyboards import (
    challenge_card_keyboard,
    chess_keyboard,
    dice_reroll_keyboard,
    dice_roll_keyboard,
    football_reroll_keyboard,
    football_roll_keyboard,
    mlbb_result_keyboard,
)
from config import settings
from db.models import (
    add_transaction,
    create_match,
    fetch_pending_chess_matches,
    fetch_stale_manual_matches,
    get_match,
    get_user,
    increment_wager_stats,
    record_game_result,
    refund_balance,
    reserve_balance,
    update_match,
)
from db.mongo import get_db
from services.house import add_house_fee
from utils import ANTI_CHEAT_WARNING, display_name, format_amount, utcnow


def sandbox_note() -> str:
    return "Sandbox mode: TON economy is disabled."


def challenge_summary(match: dict[str, Any], challenger: dict[str, Any]) -> str:
    game = str(match.get("game", "")).lower()
    mode = str(match.get("mode", "normal")).lower()
    dice_count = int(match.get("dice_count", 1))
    if game == "dice":
        game_label = f"Dice ({mode.title()}, {dice_count})"
    elif game == "football":
        game_label = f"Football ({mode.title()}, {dice_count})"
    elif game == "chess":
        game_label = "Chess"
    else:
        game_label = "MLBB"
    lines = [
        "PvP Challenge",
        f"Match ID: `{match['_id']}`",
        f"Game: {game_label}",
        f"Bet: {format_amount(float(match['amount']))} TON",
        f"Challenger: {display_name(challenger)}",
        ANTI_CHEAT_WARNING,
    ]
    if settings.sandbox_mode:
        lines.append(sandbox_note())
    return "\n".join(lines)


async def create_challenge(
    user: dict[str, Any],
    amount: float,
    game: str,
    mode: str,
    dice_count: int,
    chat_id: int,
) -> dict[str, Any]:
    if not settings.sandbox_mode:
        if not await reserve_balance(user["_id"], amount):
            raise ValueError("Insufficient balance.")
        await increment_wager_stats(user["_id"], amount)
        await add_transaction(
            user["_id"],
            "game_loss",
            -amount,
            "pending",
            metadata={"stage": "escrow"},
        )
    return await create_match(
        {
            "game": game,
            "mode": mode,
            "dice_count": int(dice_count),
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


async def post_challenge(update: Update, context: ContextTypes.DEFAULT_TYPE, match: dict[str, Any]) -> None:
    game = str(match["game"])
    mode = str(match.get("mode", "normal"))
    dice_count = int(match.get("dice_count", 1))
    amount = float(match["amount"])
    challenger = await get_user(match["challenger_id"])
    if game == "dice":
        game_label = f"Dice ({mode.title()} Mode, {dice_count} {'Die' if dice_count == 1 else 'Dice'})"
    elif game == "football":
        game_label = f"Football ({mode.title()} Mode, {dice_count} {'Shot' if dice_count == 1 else 'Shots'})"
    elif game == "chess":
        game_label = "Chess"
    else:
        game_label = "Mobile Legends"
    text = (
        f"PvP Challenge\n\n"
        f"Game: {game_label}\n"
        f"Bet: {format_amount(amount)} TON\n"
        f"Challenger: {display_name(challenger)}\n\n"
        f"Tap Accept to join this match.\n"
        f"{ANTI_CHEAT_WARNING}"
        + (f"\n{sandbox_note()}" if settings.sandbox_mode else "")
    )
    msg = await update.effective_message.reply_text(
        text,
        reply_markup=challenge_card_keyboard(match["_id"]),
    )
    await update_match(match["_id"], {"message_id": msg.message_id})


async def claim_and_activate_match(match: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    db = await get_db()
    claimed = await db.matches.find_one_and_update(
        {"_id": match["_id"], "status": "pending"},
        {"$set": {"status": "active", "opponent_id": str(user["_id"])}},
        return_document=ReturnDocument.AFTER,
    )
    if claimed is None:
        raise ValueError("Match already taken or unavailable.")
    amount = float(claimed["amount"])
    if not settings.sandbox_mode:
        if not await reserve_balance(user["_id"], amount):
            await update_match(claimed["_id"], {"status": "pending", "opponent_id": None})
            raise ValueError("Insufficient balance.")
        await increment_wager_stats(claimed["challenger_id"], amount)
        await increment_wager_stats(user["_id"], amount)
        await add_transaction(
            user["_id"],
            "game_loss",
            -amount,
            "pending",
            metadata={"stage": "escrow", "match_id": claimed["_id"]},
        )
    return claimed


async def handle_accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, match_id = (query.data or "").split(":", 1)
    opponent = await get_user(update.effective_user.id)
    if not opponent or opponent.get("is_banned"):
        await query.answer("Unavailable.", show_alert=True)
        return
    match = await get_match(match_id)
    if not match:
        await query.answer("Match not found.", show_alert=True)
        return
    if str(match["challenger_id"]) == str(opponent["_id"]):
        await query.answer("You cannot accept your own match.", show_alert=True)
        return
    db = await get_db()
    claimed = await db.matches.find_one_and_update(
        {"_id": match_id, "status": "pending"},
        {"$set": {"status": "active", "opponent_id": str(opponent["_id"])}},
        return_document=ReturnDocument.AFTER,
    )
    if claimed is None:
        await query.answer("Match already taken.", show_alert=True)
        return
    amount = float(claimed["amount"])
    if not settings.sandbox_mode:
        if not await reserve_balance(opponent["_id"], amount):
            await update_match(match_id, {"status": "pending", "opponent_id": None})
            await query.answer("Insufficient balance.", show_alert=True)
            return
        await increment_wager_stats(claimed["challenger_id"], amount)
        await increment_wager_stats(opponent["_id"], amount)
    challenger = await get_user(claimed["challenger_id"])
    lines = [
        "Match started.",
        f"Match ID: `{claimed['_id']}`",
        f"Challenger: {display_name(challenger)}",
        f"Opponent: {display_name(opponent)}",
        f"Bet: {format_amount(amount)} TON each",
    ]
    if settings.sandbox_mode:
        lines.append(sandbox_note())
    await query.message.edit_text(
        "\n".join(lines),
        reply_markup=None,
    )
    game = str(claimed["game"]).lower()
    chat_id = claimed.get("chat_id") or update.effective_chat.id
    if game == "dice":
        await start_dice_game(context, claimed, chat_id)
    elif game == "football":
        await start_football_game(context, claimed, chat_id)
    elif game == "chess":
        await start_chess_game(context, claimed, chat_id)
    elif game == "mlbb":
        await start_mlbb_game(context, claimed, chat_id)


async def handle_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, match_id = (query.data or "").split(":", 1)
    user_id = str(update.effective_user.id)
    db = await get_db()
    cancelled = await db.matches.find_one_and_update(
        {"_id": match_id, "status": "pending", "challenger_id": user_id},
        {"$set": {"status": "cancelled", "completed_at": utcnow()}},
        return_document=ReturnDocument.AFTER,
    )
    if cancelled is None:
        await query.answer("Only challenger can cancel pending match.", show_alert=True)
        return
    if not settings.sandbox_mode:
        await refund_balance(cancelled["challenger_id"], float(cancelled["amount"]))
    text = "Challenge cancelled. Bet refunded."
    if settings.sandbox_mode:
        text = f"{text}\n{sandbox_note()}"
    await query.message.edit_text(text, reply_markup=None)


async def start_dice_game(context: ContextTypes.DEFAULT_TYPE, match: dict[str, Any], chat_id: int | str) -> None:
    challenger = await get_user(match["challenger_id"])
    opponent = await get_user(match["opponent_id"])
    dice_count = int(match.get("dice_count", 1))
    mode = str(match.get("mode", "normal"))
    mode_label = "Highest wins" if mode == "normal" else "Lowest wins"
    text = (
        f"Dice Match Started\n\n"
        f"{display_name(challenger)} vs {display_name(opponent)}\n"
        f"Bet: {format_amount(float(match['amount']))} TON each\n"
        f"Mode: {mode.title()} ({mode_label})\n"
        f"Dice: {dice_count}\n\n"
        f"Both players must press Roll.\n\n"
        f"{display_name(challenger)} - Waiting\n"
        f"{display_name(opponent)} - Waiting"
        + (f"\n\n{sandbox_note()}" if settings.sandbox_mode else "")
    )
    msg = await context.bot.send_message(
        chat_id=int(chat_id),
        text=text,
        reply_markup=dice_roll_keyboard(match["_id"]),
    )
    await update_match(match["_id"], {"game_message_id": msg.message_id})


async def handle_dice_roll_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, match_id = (query.data or "").split(":", 1)
    match = await get_match(match_id)
    if not match or match.get("status") != "active" or str(match.get("game")) != "dice":
        await query.answer("Invalid match.", show_alert=True)
        return
    user_id = str(update.effective_user.id)
    if user_id not in {str(match["challenger_id"]), str(match["opponent_id"])}:
        await query.answer("You are not part of this match.", show_alert=True)
        return
    is_challenger = user_id == str(match["challenger_id"])
    if is_challenger and match.get("challenger_roll") is not None:
        await query.answer("You already rolled.", show_alert=True)
        return
    if (not is_challenger) and match.get("opponent_roll") is not None:
        await query.answer("You already rolled.", show_alert=True)
        return
    dice_count = int(match.get("dice_count", 1))
    roll = [random.randint(1, 6) for _ in range(dice_count)]
    await update_match(match_id, {"challenger_roll" if is_challenger else "opponent_roll": roll})
    await query.answer("Rolled. Waiting for opponent...")
    match = await get_match(match_id)
    challenger = await get_user(match["challenger_id"])
    opponent = await get_user(match["opponent_id"])
    if match.get("challenger_roll") is None or match.get("opponent_roll") is None:
        challenger_state = "Rolled" if match.get("challenger_roll") is not None else "Waiting"
        opponent_state = "Rolled" if match.get("opponent_roll") is not None else "Waiting"
        await context.bot.edit_message_text(
            chat_id=int(match["chat_id"]),
            message_id=match["game_message_id"],
            text=(
                f"Dice Match In Progress\n\n"
                f"{display_name(challenger)} - {challenger_state}\n"
                f"{display_name(opponent)} - {opponent_state}"
            ),
            reply_markup=dice_roll_keyboard(match["_id"]),
        )
        return
    await resolve_dice_game(context, match)


async def resolve_dice_game(context: ContextTypes.DEFAULT_TYPE, match: dict[str, Any]) -> None:
    challenger = await get_user(match["challenger_id"])
    opponent = await get_user(match["opponent_id"])
    c_roll = match["challenger_roll"] or []
    o_roll = match["opponent_roll"] or []
    c_sum = sum(c_roll)
    o_sum = sum(o_roll)
    mode = str(match.get("mode", "normal"))
    c_roll_str = ", ".join(str(x) for x in c_roll)
    o_roll_str = ", ".join(str(x) for x in o_roll)
    winner_id: str | None
    if mode == "normal":
        winner_id = match["challenger_id"] if c_sum > o_sum else (match["opponent_id"] if o_sum > c_sum else None)
    else:
        winner_id = match["challenger_id"] if c_sum < o_sum else (match["opponent_id"] if o_sum < c_sum else None)
    if winner_id is None:
        await update_match(match["_id"], {"challenger_roll": None, "opponent_roll": None})
        await context.bot.edit_message_text(
            chat_id=int(match["chat_id"]),
            message_id=match["game_message_id"],
            text=(
                f"It's a Tie\n\n"
                f"{display_name(challenger)} rolled: [{c_roll_str}] = {c_sum}\n"
                f"{display_name(opponent)} rolled: [{o_roll_str}] = {o_sum}\n\n"
                f"Press Roll Again to continue."
            ),
            reply_markup=dice_reroll_keyboard(match["_id"]),
        )
        return
    loser_id = match["opponent_id"] if winner_id == match["challenger_id"] else match["challenger_id"]
    amount = float(match["amount"])
    payout = round(amount * 2 * 0.95, 8)
    fee = round(amount * 2 * 0.05, 8)
    if not settings.sandbox_mode:
        await record_game_result(winner_id, loser_id, amount, payout)
        await add_house_fee(fee)
    await update_match(
        match["_id"],
        {"status": "completed", "winner_id": winner_id, "completed_at": utcnow()},
    )
    winner = await get_user(winner_id)
    await context.bot.edit_message_text(
        chat_id=int(match["chat_id"]),
        message_id=match["game_message_id"],
        text=(
            f"Dice Results\n\n"
            f"{display_name(challenger)} rolled: [{c_roll_str}] = {c_sum}\n"
            f"{display_name(opponent)} rolled: [{o_roll_str}] = {o_sum}\n\n"
            f"Winner: {display_name(winner)} (+{format_amount(payout)} TON)\n"
            f"House fee: {format_amount(fee)} TON\n\n"
            f"{ANTI_CHEAT_WARNING}"
            + (f"\n{sandbox_note()}" if settings.sandbox_mode else "")
        ),
        reply_markup=None,
    )


async def start_football_game(context: ContextTypes.DEFAULT_TYPE, match: dict[str, Any], chat_id: int | str) -> None:
    challenger = await get_user(match["challenger_id"])
    opponent = await get_user(match["opponent_id"])
    shots = int(match.get("dice_count", 1))
    mode = str(match.get("mode", "normal"))
    mode_label = "Highest wins" if mode == "normal" else "Lowest wins"
    text = (
        f"Football Match Started\n\n"
        f"{display_name(challenger)} vs {display_name(opponent)}\n"
        f"Bet: {format_amount(float(match['amount']))} TON each\n"
        f"Mode: {mode.title()} ({mode_label})\n"
        f"Shots: {shots}\n\n"
        f"Both players must press Shot.\n\n"
        f"{display_name(challenger)} - Waiting\n"
        f"{display_name(opponent)} - Waiting"
        + (f"\n\n{sandbox_note()}" if settings.sandbox_mode else "")
    )
    msg = await context.bot.send_message(
        chat_id=int(chat_id),
        text=text,
        reply_markup=football_roll_keyboard(match["_id"]),
    )
    await update_match(match["_id"], {"game_message_id": msg.message_id})


async def handle_football_roll_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, match_id = (query.data or "").split(":", 1)
    match = await get_match(match_id)
    if not match or match.get("status") != "active" or str(match.get("game")) != "football":
        await query.answer("Invalid match.", show_alert=True)
        return
    user_id = str(update.effective_user.id)
    if user_id not in {str(match["challenger_id"]), str(match["opponent_id"])}:
        await query.answer("You are not part of this match.", show_alert=True)
        return
    is_challenger = user_id == str(match["challenger_id"])
    if is_challenger and match.get("challenger_roll") is not None:
        await query.answer("You already kicked.", show_alert=True)
        return
    if (not is_challenger) and match.get("opponent_roll") is not None:
        await query.answer("You already kicked.", show_alert=True)
        return
    shots = int(match.get("dice_count", 1))
    roll = [random.randint(1, 5) for _ in range(shots)]
    await update_match(match_id, {"challenger_roll" if is_challenger else "opponent_roll": roll})
    await query.answer("Shot recorded. Waiting for opponent...")
    match = await get_match(match_id)
    challenger = await get_user(match["challenger_id"])
    opponent = await get_user(match["opponent_id"])
    if match.get("challenger_roll") is None or match.get("opponent_roll") is None:
        challenger_state = "Kicked" if match.get("challenger_roll") is not None else "Waiting"
        opponent_state = "Kicked" if match.get("opponent_roll") is not None else "Waiting"
        await context.bot.edit_message_text(
            chat_id=int(match["chat_id"]),
            message_id=match["game_message_id"],
            text=(
                f"Football Match In Progress\n\n"
                f"{display_name(challenger)} - {challenger_state}\n"
                f"{display_name(opponent)} - {opponent_state}"
            ),
            reply_markup=football_roll_keyboard(match["_id"]),
        )
        return
    await resolve_football_game(context, match)


async def resolve_football_game(context: ContextTypes.DEFAULT_TYPE, match: dict[str, Any]) -> None:
    challenger = await get_user(match["challenger_id"])
    opponent = await get_user(match["opponent_id"])
    c_roll = match["challenger_roll"] or []
    o_roll = match["opponent_roll"] or []
    c_sum = sum(c_roll)
    o_sum = sum(o_roll)
    mode = str(match.get("mode", "normal"))
    c_roll_str = ", ".join(str(x) for x in c_roll)
    o_roll_str = ", ".join(str(x) for x in o_roll)
    winner_id: str | None
    if mode == "normal":
        winner_id = match["challenger_id"] if c_sum > o_sum else (match["opponent_id"] if o_sum > c_sum else None)
    else:
        winner_id = match["challenger_id"] if c_sum < o_sum else (match["opponent_id"] if o_sum < c_sum else None)
    if winner_id is None:
        await update_match(match["_id"], {"challenger_roll": None, "opponent_roll": None})
        await context.bot.edit_message_text(
            chat_id=int(match["chat_id"]),
            message_id=match["game_message_id"],
            text=(
                f"It's a Tie\n\n"
                f"{display_name(challenger)} kicked: [{c_roll_str}] = {c_sum}\n"
                f"{display_name(opponent)} kicked: [{o_roll_str}] = {o_sum}\n\n"
                f"Press Shoot Again to continue."
            ),
            reply_markup=football_reroll_keyboard(match["_id"]),
        )
        return
    loser_id = match["opponent_id"] if winner_id == match["challenger_id"] else match["challenger_id"]
    amount = float(match["amount"])
    payout = round(amount * 2 * 0.95, 8)
    fee = round(amount * 2 * 0.05, 8)
    if not settings.sandbox_mode:
        await record_game_result(winner_id, loser_id, amount, payout)
        await add_house_fee(fee)
    await update_match(
        match["_id"],
        {"status": "completed", "winner_id": winner_id, "completed_at": utcnow()},
    )
    winner = await get_user(winner_id)
    await context.bot.edit_message_text(
        chat_id=int(match["chat_id"]),
        message_id=match["game_message_id"],
        text=(
            f"Football Results\n\n"
            f"{display_name(challenger)} kicked: [{c_roll_str}] = {c_sum}\n"
            f"{display_name(opponent)} kicked: [{o_roll_str}] = {o_sum}\n\n"
            f"Winner: {display_name(winner)} (+{format_amount(payout)} TON)\n"
            f"House fee: {format_amount(fee)} TON\n\n"
            f"{ANTI_CHEAT_WARNING}"
            + (f"\n{sandbox_note()}" if settings.sandbox_mode else "")
        ),
        reply_markup=None,
    )


async def start_chess_game(context: ContextTypes.DEFAULT_TYPE, match: dict[str, Any], chat_id: int | str) -> None:
    challenger = await get_user(match["challenger_id"])
    opponent = await get_user(match["opponent_id"])
    text = (
        f"Chess Match Started\n\n"
        f"{display_name(challenger)} (White) vs {display_name(opponent)} (Black)\n"
        f"Bet: {format_amount(float(match['amount']))} TON\n\n"
        f"Open the board to play your moves.\n"
        f"Match expires in 2 hours if not completed."
        + (f"\n{sandbox_note()}" if settings.sandbox_mode else "")
    )
    await context.bot.send_message(
        chat_id=int(match["challenger_id"]),
        text=text,
        reply_markup=chess_keyboard(match["_id"], match["challenger_id"], settings.webhook_url),
    )
    await context.bot.send_message(
        chat_id=int(match["opponent_id"]),
        text=text,
        reply_markup=chess_keyboard(match["_id"], match["opponent_id"], settings.webhook_url),
    )
    if str(chat_id) not in {str(match["challenger_id"]), str(match["opponent_id"])}:
        await context.bot.send_message(chat_id=int(chat_id), text=f"Chess board links sent for match `{match['_id']}`.")


async def start_mlbb_game(context: ContextTypes.DEFAULT_TYPE, match: dict[str, Any], chat_id: int | str) -> None:
    challenger = await get_user(match["challenger_id"])
    opponent = await get_user(match["opponent_id"])
    text = (
        f"MLBB Match Started\n\n"
        f"{display_name(challenger)} vs {display_name(opponent)}\n"
        f"Bet: {format_amount(float(match['amount']))} TON\n\n"
        f"Play your Mobile Legends match and report result below.\n\n"
        f"{ANTI_CHEAT_WARNING}"
        + (f"\n{sandbox_note()}" if settings.sandbox_mode else "")
    )
    msg = await context.bot.send_message(
        chat_id=int(chat_id),
        text=text,
        reply_markup=mlbb_result_keyboard(match["_id"]),
    )
    await update_match(match["_id"], {"game_message_id": msg.message_id})


async def handle_mlbb_result_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, match_id, result = (query.data or "").split(":", 2)
    match = await get_match(match_id)
    if not match or match.get("status") != "active" or str(match.get("game")) != "mlbb":
        await query.answer("Invalid match.", show_alert=True)
        return
    user_id = str(update.effective_user.id)
    if user_id not in {str(match["challenger_id"]), str(match["opponent_id"])}:
        await query.answer("You are not part of this match.", show_alert=True)
        return
    field = "challenger_result" if user_id == str(match["challenger_id"]) else "opponent_result"
    if match.get(field):
        await query.answer("Already reported.", show_alert=True)
        return
    await update_match(match_id, {field: result})
    await query.answer("Result recorded.")
    match = await get_match(match_id)
    challenger = await get_user(match["challenger_id"])
    opponent = await get_user(match["opponent_id"])
    if not match.get("challenger_result") or not match.get("opponent_result"):
        challenger_state = "Reported" if match.get("challenger_result") else "Waiting"
        opponent_state = "Reported" if match.get("opponent_result") else "Waiting"
        await context.bot.edit_message_text(
            chat_id=int(match["chat_id"]),
            message_id=match["game_message_id"],
            text=(
                f"MLBB Result Check\n\n"
                f"{display_name(challenger)} - {challenger_state}\n"
                f"{display_name(opponent)} - {opponent_state}"
            ),
            reply_markup=mlbb_result_keyboard(match["_id"]),
        )
        return
    c_result = str(match["challenger_result"])
    o_result = str(match["opponent_result"])
    if c_result == "win" and o_result == "lose":
        winner_id = match["challenger_id"]
    elif c_result == "lose" and o_result == "win":
        winner_id = match["opponent_id"]
    else:
        await update_match(match_id, {"status": "disputed"})
        await context.bot.edit_message_text(
            chat_id=int(match["chat_id"]),
            message_id=match["game_message_id"],
            text=f"MLBB match `{match_id}` is disputed. Admin review required.",
            reply_markup=None,
        )
        for admin_id in settings.admin_ids:
            try:
                await context.bot.send_message(
                    chat_id=int(admin_id),
                    text=f"Disputed MLBB match `{match_id}`. Resolve with /resolve {match_id} <winner_user_id>",
                )
            except Exception:
                pass
        return
    settled, payout, fee = await settle_match(match, str(winner_id))
    winner = await get_user(winner_id)
    await context.bot.edit_message_text(
        chat_id=int(settled["chat_id"]),
        message_id=settled["game_message_id"],
        text=(
            f"MLBB Results\n\n"
            f"Winner: {display_name(winner)} (+{format_amount(payout)} TON)\n"
            f"House fee: {format_amount(fee)} TON\n\n"
            f"{ANTI_CHEAT_WARNING}"
            + (f"\n{sandbox_note()}" if settings.sandbox_mode else "")
        ),
        reply_markup=None,
    )


async def settle_match(match: dict[str, Any], winner_id: str) -> tuple[dict[str, Any], float, float]:
    loser_id = match["opponent_id"] if winner_id == match["challenger_id"] else match["challenger_id"]
    amount = float(match["amount"])
    payout = round(amount * 2 * 0.95, 8)
    fee = round(amount * 2 * 0.05, 8)
    if not settings.sandbox_mode:
        await record_game_result(winner_id, loser_id, amount, payout)
        await add_house_fee(fee)
    updated = await update_match(
        match["_id"],
        {
            "status": "completed",
            "winner_id": winner_id,
            "completed_at": utcnow(),
        },
    )
    return updated, payout, fee


async def cancel_match_and_refund(match: dict[str, Any]) -> None:
    await update_match(
        match["_id"],
        {
            "status": "cancelled",
            "completed_at": utcnow(),
        },
    )
    if not settings.sandbox_mode:
        await refund_balance(match["challenger_id"], float(match["amount"]))
        if match.get("opponent_id"):
            await refund_balance(match["opponent_id"], float(match["amount"]))


async def roll_competitive_dice(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    emoji: str,
    dice_count: int,
) -> tuple[int, int]:
    while True:
        challenger_total = 0
        opponent_total = 0
        for _ in range(dice_count):
            first_roll = await context.bot.send_dice(chat_id=update.effective_chat.id, emoji=emoji)
            second_roll = await context.bot.send_dice(chat_id=update.effective_chat.id, emoji=emoji)
            challenger_total += int(first_roll.dice.value)
            opponent_total += int(second_roll.dice.value)
        if challenger_total != opponent_total:
            return challenger_total, opponent_total
        await update.effective_message.reply_text("Tie detected, re-rolling automatically.")


async def mark_stale_mlbb_matches_disputed(application) -> None:
    cutoff = utcnow() - timedelta(hours=24)
    matches = await fetch_stale_manual_matches(cutoff)
    for match in matches:
        updated = await update_match(match["_id"], {"status": "disputed"})
        if not updated:
            continue
        for chat_id in {updated.get("chat_id"), updated.get("challenger_id"), updated.get("opponent_id")}:
            if chat_id:
                try:
                    await application.bot.send_message(
                        chat_id=int(chat_id),
                        text=f"MLBB match `{updated['_id']}` expired after 24 hours and is now disputed.",
                    )
                except Exception:
                    pass
        for admin_id in settings.admin_ids:
            try:
                await application.bot.send_message(
                    chat_id=int(admin_id),
                    text=f"MLBB match `{updated['_id']}` expired after 24 hours. Resolve with /resolve {updated['_id']} <winner_user_id>",
                )
            except Exception:
                pass


async def expire_old_games(application) -> None:
    timeout_before = utcnow() - timedelta(hours=2)
    matches = await fetch_pending_chess_matches(timeout_before)
    for match in matches:
        await cancel_match_and_refund(match)
        message = f"Chess match `{match['_id']}` expired after 2 hours and both players were refunded."
        for chat_id in {match.get("chat_id"), match.get("challenger_id"), match.get("opponent_id")}:
            if chat_id:
                try:
                    await application.bot.send_message(chat_id=int(chat_id), text=message)
                except Exception:
                    pass
    await mark_stale_mlbb_matches_disputed(application)
