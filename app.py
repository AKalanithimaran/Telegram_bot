from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Route
from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from bot import admin as admin_handlers
from bot import games
from bot.games import cancel_match_and_refund, expire_old_games, settle_match
from bot.handlers import (
    accept_command,
    balance_command,
    challenge_command,
    deposit_command,
    fallback_text_handler,
    history_command,
    leaderboard_command,
    menu_callback,
    profile_command,
    result_command,
    setmlbb_command,
    start_command,
    tip_command,
    withdraw_command,
)
from config import logger, settings
from db.models import claim_ton_deposit, get_match, get_user
from db.mongo import mongo
from services.ton import extract_amount, extract_comment, extract_ton_lt, extract_tx_hash, is_incoming, safe_fetch_recent_transactions
from utils import ANTI_CHEAT_WARNING, display_name, format_amount

ALLOWED_UPDATES = [
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "callback_query",
    "inline_query",
    "chosen_inline_result",
    "my_chat_member",
    "chat_member",
]

telegram_app: Application | None = None


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, PermissionError) and isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text(str(context.error))
        return
    logger.exception("Unhandled error: %s", context.error)


async def poll_ton_deposits(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        if not settings.ton_enabled:
            return
        transactions = await safe_fetch_recent_transactions(limit=20)
        if not transactions:
            return
        for tx in transactions:
            if not is_incoming(tx):
                continue
            ton_lt = extract_ton_lt(tx)
            memo = extract_comment(tx)
            if not ton_lt or not memo or not memo.isdigit():
                continue
            user = await get_user(memo)
            if not user:
                continue
            amount = extract_amount(tx)
            tx_hash = extract_tx_hash(tx)
            if not await claim_ton_deposit(user_id=memo, amount=amount, ton_lt=ton_lt, tx_hash=tx_hash):
                continue
            try:
                await context.bot.send_message(
                    chat_id=int(memo),
                    text=f"✅ TON deposit confirmed: +{format_amount(amount)} TON added to your balance.",
                )
            except Exception:
                pass
    except Exception as exc:
        logger.exception("poll_ton_deposits error: %s", exc)


async def game_expiry(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        context.application.admin_ids = settings.admin_ids
        await expire_old_games(context.application)
    except Exception as exc:
        logger.exception("game_expiry error: %s", exc)


def build_telegram_application() -> Application:
    application = Application.builder().token(settings.telegram_bot_token).updater(None).build()
    application.admin_ids = settings.admin_ids
    user_handlers = [
        ("start", start_command),
        ("deposit", deposit_command),
        ("withdraw", withdraw_command),
        ("balance", balance_command),
        ("tip", tip_command),
        ("challenge", challenge_command),
        ("accept", accept_command),
        ("result", result_command),
        ("profile", profile_command),
        ("history", history_command),
        ("leaderboard", leaderboard_command),
        ("setmlbb", setmlbb_command),
    ]
    admin_command_map = [
        ("add_balance", admin_handlers.add_balance_command),
        ("deduct_balance", admin_handlers.deduct_balance_command),
        ("approve_withdrawal", admin_handlers.approve_withdrawal_command),
        ("reject_withdrawal", admin_handlers.reject_withdrawal_command),
        ("approve_deposit", admin_handlers.approve_deposit_command),
        ("resolve", admin_handlers.resolve_command),
        ("admin_stats", admin_handlers.admin_stats_command),
        ("wager_report", admin_handlers.wager_report_command),
        ("admin_user", admin_handlers.admin_user_command),
        ("admin_matches", admin_handlers.admin_matches_command),
        ("admin_ban", admin_handlers.admin_ban_command),
        ("admin_unban", admin_handlers.admin_unban_command),
        ("set_fee", admin_handlers.set_fee_command),
        ("set_min_wager", admin_handlers.set_min_wager_command),
        ("set_deposit_address", admin_handlers.set_deposit_address_command),
        ("admin_refund", admin_handlers.admin_refund_command),
        ("admin_balance", admin_handlers.admin_balance_command),
    ]
    for name, handler in user_handlers + admin_command_map:
        application.add_handler(CommandHandler(name, handler))
    application.add_handler(CallbackQueryHandler(games.handle_accept_callback, pattern=r"^accept:"))
    application.add_handler(CallbackQueryHandler(games.handle_cancel_callback, pattern=r"^cancel:"))
    application.add_handler(CallbackQueryHandler(games.handle_dice_roll_callback, pattern=r"^dice_roll:"))
    application.add_handler(CallbackQueryHandler(games.handle_football_roll_callback, pattern=r"^football_roll:"))
    application.add_handler(CallbackQueryHandler(games.handle_mlbb_result_callback, pattern=r"^mlbb_result:"))
    application.add_handler(CallbackQueryHandler(menu_callback, pattern=r"^(menu:|deposit:|games:)"))
    application.add_handler(CallbackQueryHandler(admin_handlers.admin_withdraw_callback, pattern=r"^admin_withdraw_(approve|reject):"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_text_handler))
    application.add_error_handler(error_handler)
    if application.job_queue is None:
        raise RuntimeError(
            "python-telegram-bot was installed without job-queue support. "
            "Install with: python-telegram-bot[job-queue]==20.8"
        )
    if settings.ton_enabled:
        application.job_queue.run_repeating(poll_ton_deposits, interval=30, first=10, name="ton_poll")
    else:
        logger.info("TON polling disabled for app_env=%s", settings.app_env)
    application.job_queue.run_repeating(game_expiry, interval=60, first=30, name="game_expiry")
    return application


async def health(_: Request) -> JSONResponse:
    return JSONResponse({"ok": True, "service": "telegram-gambling-bot"})


async def webhook(request: Request) -> JSONResponse:
    if telegram_app is None:
        return JSONResponse({"ok": False, "error": "bot_not_ready"}, status_code=503)
    if settings.webhook_secret:
        token = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if token != settings.webhook_secret:
            return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    payload = await request.json()
    update = Update.de_json(payload, telegram_app.bot)
    await telegram_app.process_update(update)
    return JSONResponse({"ok": True})


async def chess_page(request: Request) -> FileResponse:
    return FileResponse(Path(__file__).with_name("chess.html"))


async def chess_result(request: Request) -> JSONResponse:
    if telegram_app is None:
        return JSONResponse({"ok": False, "error": "bot_not_ready"}, status_code=503)
    payload = await request.json()
    match_id = str(payload.get("match_id", "")).strip()
    winner_user_id = str(payload.get("winner_user_id", "")).strip()
    match = await get_match(match_id)
    if not match or match["game"] != "chess":
        return JSONResponse({"ok": False, "error": "invalid_match"}, status_code=404)
    if match["status"] != "active":
        return JSONResponse({"ok": False, "error": "match_not_active"}, status_code=400)
    if winner_user_id == "draw":
        await cancel_match_and_refund(match)
        draw_text = "\n".join(
            [
                f"Chess match `{match['_id']}` ended in a draw.",
                "Bets refunded to both players.",
            ]
        )
        for chat_id in {match.get("chat_id"), match.get("challenger_id"), match.get("opponent_id")}:
            if chat_id:
                try:
                    await telegram_app.bot.send_message(chat_id=int(chat_id), text=draw_text)
                except Exception:
                    pass
        return JSONResponse({"ok": True, "result": "draw"})
    if winner_user_id not in {str(match["challenger_id"]), str(match["opponent_id"])}:
        return JSONResponse({"ok": False, "error": "invalid_winner"}, status_code=400)
    settled, payout, fee = await settle_match(match, winner_user_id)
    winner = await get_user(winner_user_id)
    text = "\n".join(
        [
            f"♟️ Chess match `{settled['_id']}` completed!",
            f"🏆 Winner: {display_name(winner)}",
            f"💰 Payout: {format_amount(payout)} TON",
            f"🏦 House fee: {format_amount(fee)} TON",
            ANTI_CHEAT_WARNING,
        ]
    )
    for chat_id in {settled.get("chat_id"), settled.get("challenger_id"), settled.get("opponent_id")}:
        if chat_id:
            try:
                await telegram_app.bot.send_message(chat_id=int(chat_id), text=text)
            except Exception:
                pass
    return JSONResponse({"ok": True, "match_id": settled["_id"], "winner_user_id": winner_user_id})


async def chess_state(request: Request) -> JSONResponse:
    match_id = request.query_params.get("match_id", "")
    match = await get_match(match_id)
    if not match or match["game"] != "chess":
        return JSONResponse({"ok": False}, status_code=404)
    return JSONResponse(
        {
            "ok": True,
            "fen": match.get("fen", "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"),
            "turn": match.get("turn", "white"),
            "challenger_id": match["challenger_id"],
            "opponent_id": match["opponent_id"],
            "challenger_color": match.get("challenger_color", "white"),
            "opponent_color": match.get("opponent_color", "black"),
            "move_history": match.get("move_history", []),
            "status": match["status"],
        }
    )


async def chess_move(request: Request) -> JSONResponse:
    payload = await request.json()
    match_id = str(payload.get("match_id", "")).strip()
    user_id = str(payload.get("user_id", "")).strip()
    move = str(payload.get("move", "")).strip()
    match = await get_match(match_id)
    if not match or match["game"] != "chess" or match["status"] != "active":
        return JSONResponse({"ok": False, "error": "invalid_match"}, status_code=400)
    turn = match.get("turn", "white")
    challenger_color = match.get("challenger_color", "white")
    if turn == challenger_color:
        expected_user = str(match["challenger_id"])
    else:
        expected_user = str(match["opponent_id"])
    if user_id != expected_user:
        return JSONResponse({"ok": False, "error": "not_your_turn"}, status_code=403)
    history = list(match.get("move_history", []))
    history.append(move)
    new_turn = "black" if turn == "white" else "white"
    new_fen = str(payload.get("fen", "")).strip()
    await update_match(
        match_id,
        {
            "fen": new_fen,
            "turn": new_turn,
            "move_history": history,
        },
    )
    return JSONResponse({"ok": True, "fen": new_fen, "turn": new_turn})


@asynccontextmanager
async def lifespan(_: Starlette):
    global telegram_app
    await mongo.connect()
    telegram_app = build_telegram_application()
    await telegram_app.initialize()
    await telegram_app.start()
    webhook_target = f"{settings.webhook_url}/webhook"
    try:
        await telegram_app.bot.set_webhook(
            url=webhook_target,
            allowed_updates=ALLOWED_UPDATES,
            secret_token=settings.webhook_secret or None,
            drop_pending_updates=True,
        )
        logger.info("Webhook set → %s", webhook_target)
    except Exception as exc:
        logger.error("Failed to set webhook: %s", exc)
    try:
        yield
    finally:
        logger.info("Shutting down...")
        if telegram_app is not None:
            try:
                await telegram_app.bot.delete_webhook(drop_pending_updates=False)
            except Exception:
                pass
            await telegram_app.stop()
            await telegram_app.shutdown()
        await mongo.close()


starlette_app = Starlette(
    debug=False,
    routes=[
        Route("/", health, methods=["GET"]),
        Route("/health", health, methods=["GET"]),
        Route("/webhook", webhook, methods=["POST"]),
        Route("/chess", chess_page, methods=["GET"]),
        Route("/chess_state", chess_state, methods=["GET"]),
        Route("/chess_move", chess_move, methods=["POST"]),
        Route("/chess_result", chess_result, methods=["POST"]),
    ],
    lifespan=lifespan,
)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:starlette_app", host="0.0.0.0", port=port, log_level="info")
