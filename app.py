from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, PlainTextResponse
from starlette.routing import Route
from telegram import Update
from telegram.constants import UpdateType
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
import uvicorn

from bot import admin as admin_handlers
from bot.games import cancel_match_and_refund, settle_match
from bot.handlers import (
    accept_callback,
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
from db.models import add_transaction, get_match, get_user, update_match
from db.mongo import mongo
from services.ton import extract_amount, extract_comment, extract_ton_lt, extract_tx_hash, is_incoming, safe_fetch_recent_transactions
from utils import ANTI_CHEAT_WARNING, display_name, format_amount, utcnow

telegram_app: Application | None = None


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, PermissionError) and isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text(str(context.error))
        return
    logger.exception("Unhandled error: %s", context.error)


async def poll_ton_deposits(context: ContextTypes.DEFAULT_TYPE) -> None:
    db = await mongo.connect()
    transactions = await safe_fetch_recent_transactions(limit=20)
    if not transactions:
        return
    for tx in transactions:
        if not is_incoming(tx):
            continue
        ton_lt = extract_ton_lt(tx)
        if not ton_lt:
            continue
        already = await db.transactions.find_one({"ton_lt": ton_lt})
        if already:
            continue
        memo = extract_comment(tx)
        if not memo.isdigit():
            continue
        user = await get_user(memo)
        if not user:
            continue
        amount = extract_amount(tx)
        tx_hash = extract_tx_hash(tx)
        await db.users.update_one({"_id": str(memo)}, {"$inc": {"balance": amount}})
        await db.house.update_one({"_id": "singleton"}, {"$inc": {"total_deposited": amount}})
        await add_transaction(
            memo,
            "deposit",
            amount,
            "completed",
            crypto="TON",
            tx_hash=tx_hash,
            ton_lt=ton_lt,
        )
        await context.bot.send_message(
            chat_id=int(memo),
            text=f"TON deposit confirmed: +{format_amount(amount)} TON.",
        )


async def expire_unfinished_games(context: ContextTypes.DEFAULT_TYPE) -> None:
    db = await mongo.connect()
    chess_cutoff = utcnow() - timedelta(hours=2)
    async for match in db.matches.find({"game": "chess", "status": "active", "created_at": {"$lte": chess_cutoff}}):
        await cancel_match_and_refund(match)
        for chat_id in {match.get("chat_id"), match.get("challenger_id"), match.get("opponent_id")}:
            if chat_id:
                await context.bot.send_message(
                    chat_id=int(chat_id),
                    text=f"Chess match `{match['_id']}` expired after 2 hours and both players were refunded.",
                )


def build_telegram_application() -> Application:
    application = Application.builder().token(settings.telegram_bot_token).updater(None).build()
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
    application.add_handler(CallbackQueryHandler(menu_callback, pattern=r"^(menu:|deposit:|games:)"))
    application.add_handler(CallbackQueryHandler(accept_callback, pattern=r"^accept:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_text_handler))
    application.add_error_handler(error_handler)
    if application.job_queue is None:
        raise RuntimeError(
            "python-telegram-bot was installed without job-queue support. "
            "Install with python-telegram-bot[job-queue]==20.8."
        )
    application.job_queue.run_repeating(poll_ton_deposits, interval=30, first=10, name="ton_poll")
    application.job_queue.run_repeating(expire_unfinished_games, interval=60, first=30, name="game_expiry")
    return application


async def health(_: Request) -> JSONResponse:
    return JSONResponse({"ok": True, "service": "telegram-gambling-bot"})


async def webhook(request: Request) -> JSONResponse:
    if telegram_app is None:
        return JSONResponse({"ok": False, "error": "bot_not_ready"}, status_code=503)
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
    if winner_user_id not in {str(match["challenger_id"]), str(match["opponent_id"])}:
        return JSONResponse({"ok": False, "error": "invalid_winner"}, status_code=400)
    settled, payout, fee = await settle_match(match, winner_user_id)
    winner = await get_user(winner_user_id)
    text = "\n".join(
        [
            f"Chess match `{settled['_id']}` completed.",
            f"Winner: {display_name(winner)}",
            f"Payout: {format_amount(payout)} TON",
            f"House fee: {format_amount(fee)} TON",
            ANTI_CHEAT_WARNING,
        ]
    )
    for chat_id in {settled.get("chat_id"), settled.get("challenger_id"), settled.get("opponent_id")}:
        if chat_id:
            await telegram_app.bot.send_message(chat_id=int(chat_id), text=text)
    return JSONResponse({"ok": True, "match_id": settled["_id"], "winner_user_id": winner_user_id})


@asynccontextmanager
async def lifespan(_: Starlette):
    global telegram_app
    await mongo.connect()
    telegram_app = build_telegram_application()
    await telegram_app.initialize()
    await telegram_app.start()
    webhook_target = f"{settings.webhook_url}/webhook"
    await telegram_app.bot.set_webhook(
        url=webhook_target,
        secret_token=settings.webhook_secret or None,
        allowed_updates=list(UpdateType.ALL_TYPES),
        drop_pending_updates=True,
    )
    logger.info("Webhook set to %s", webhook_target)
    try:
        yield
    finally:
        if telegram_app is not None:
            await telegram_app.bot.delete_webhook(drop_pending_updates=False)
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
        Route("/chess_result", chess_result, methods=["POST"]),
    ],
    lifespan=lifespan,
)


if __name__ == "__main__":
    uvicorn.run("app:starlette_app", host="0.0.0.0", port=settings.port, log_level="info")
