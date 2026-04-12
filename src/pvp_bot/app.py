import asyncio

import httpx
from telegram import Update
from telegram.error import TelegramError
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

from .config import BOT_TOKEN, HTTP_TIMEOUT, PORT, WEBHOOK_SECRET_TOKEN, WEBHOOK_URL, logger, resolved_app_mode
from .database import init_db
from .handlers import (
    accept_command,
    admin_balance_command,
    admin_ban_command,
    admin_matches_command,
    admin_refund_command,
    admin_stats_command,
    admin_unban_command,
    admin_user_command,
    approve_command,
    balance_command,
    challenge_command,
    deposit_command,
    profile_command,
    reject_command,
    resolve_command,
    result_command,
    setmlbb_command,
    start_command,
    verify_command,
    withdraw_command,
)
from .jobs import deposit_and_payment_job, match_timeout_job
from .telegram_helpers import safe_reply


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


async def post_init(application: Application) -> None:
    if resolved_app_mode() != "polling":
        return
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Existing webhook cleared before polling startup.")
    except TelegramError as exc:
        logger.warning("Failed to clear webhook before polling: %s", exc)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled application error: %s", context.error)
    if isinstance(update, Update) and update.effective_message:
        await safe_reply(update, "⚠️ Something went wrong while processing your request. Please try again.")


def build_application() -> Application:
    application = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    for name, handler in [
        ("start", start_command),
        ("setmlbb", setmlbb_command),
        ("verify", verify_command),
        ("approve", approve_command),
        ("reject", reject_command),
        ("deposit", deposit_command),
        ("withdraw", withdraw_command),
        ("balance", balance_command),
        ("challenge", challenge_command),
        ("accept", accept_command),
        ("result", result_command),
        ("resolve", resolve_command),
        ("profile", profile_command),
        ("admin_stats", admin_stats_command),
        ("admin_matches", admin_matches_command),
        ("admin_user", admin_user_command),
        ("admin_balance", admin_balance_command),
        ("admin_refund", admin_refund_command),
        ("admin_ban", admin_ban_command),
        ("admin_unban", admin_unban_command),
    ]:
        application.add_handler(CommandHandler(name, handler))
    application.add_error_handler(error_handler)
    if application.job_queue is None:
        raise RuntimeError("Job queue is unavailable. Install python-telegram-bot with job-queue extras.")
    application.job_queue.run_repeating(deposit_and_payment_job, interval=60, first=15, name="deposit_and_payment_job")
    application.job_queue.run_repeating(match_timeout_job, interval=300, first=60, name="match_timeout_job")
    return application


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing.")
    init_db()
    logger.info("Database initialized.")
    application = build_application()
    mode = resolved_app_mode()
    logger.info("Bot starting in %s mode...", mode)
    if mode == "webhook":
        if not WEBHOOK_URL:
            raise RuntimeError("WEBHOOK_URL is required for webhook mode.")
        webhook_path = f"/telegram/{BOT_TOKEN}"
        webhook_target = f"{WEBHOOK_URL}{webhook_path}"
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=webhook_path.lstrip("/"),
            webhook_url=webhook_target,
            secret_token=WEBHOOK_SECRET_TOKEN or None,
            drop_pending_updates=True,
        )
        return
    asyncio.run(force_delete_webhook())
    asyncio.set_event_loop(asyncio.new_event_loop())
    application.run_polling(drop_pending_updates=True)
