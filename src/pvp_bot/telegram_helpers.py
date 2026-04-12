import sqlite3
from typing import Optional

from telegram import Update
from telegram.error import TelegramError

from .config import ADMIN_ID, logger
from .database import get_match, get_user, sync_user_from_update, update_match_after_challenge
from .utils import is_group, is_private, username_label


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
    from .database import get_user  # local import avoids cycles

    user = get_user(update.effective_user.id)
    if user is None:
        raise RuntimeError("User record could not be created.")
    return user


async def post_waiting_challenge(bot, match_id: int, challenge_text_builder) -> Optional[int]:
    match = get_match(match_id)
    if not match:
        return None
    challenger = get_user(match["player1"])
    if not challenger:
        return None
    text = challenge_text_builder(match, challenger)
    try:
        sent = await bot.send_message(chat_id=match["group_chat_id"], text=text)
        update_match_after_challenge(match_id, sent.message_id)
        await pin_message_if_possible(bot, match["group_chat_id"], sent.message_id)
        return sent.message_id
    except TelegramError as exc:
        logger.warning("Failed to post challenge %s: %s", match_id, exc)
        return None
