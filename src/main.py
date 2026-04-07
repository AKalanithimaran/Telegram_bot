import os
import random
from typing import Optional

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "123456789"))
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
PORT = int(os.getenv("PORT", "1000"))

def get_db_connection() -> psycopg.Connection:
    if DATABASE_URL:
        return psycopg.connect(DATABASE_URL, row_factory=dict_row)

    host = os.getenv("PGHOST", "").strip()
    dbname = os.getenv("PGDATABASE", "").strip()
    user = os.getenv("PGUSER", "").strip()
    password = os.getenv("PGPASSWORD", "").strip()
    port = os.getenv("PGPORT", "5432").strip()
    sslmode = os.getenv("PGSSLMODE", "require").strip()

    if not all([host, dbname, user, password]):
        raise RuntimeError("Missing database configuration. Set DATABASE_URL or PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    return psycopg.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
        sslmode=sslmode,
        row_factory=dict_row,
    )


def init_db() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    mlbb_id TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS matches (
                    match_id BIGSERIAL PRIMARY KEY,
                    player1 BIGINT,
                    player2 BIGINT,
                    game TEXT,
                    amount INTEGER,
                    status TEXT,
                    result1 TEXT,
                    result2 TEXT
                )
                """
            )


def is_private_chat(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == "private")


def is_group_chat(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type in {"group", "supergroup"})


def user_label(user_id: int) -> str:
    return f"User {user_id}"


async def group_user_display(update: Update, user_id: int) -> str:
    try:
        member = await update.effective_chat.get_member(user_id)
        user = member.user
        if user.username:
            return f"@{user.username}"
        if user.full_name:
            return user.full_name
    except Exception:
        pass
    return user_label(user_id)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Welcome to PvP Bot 🎮")


async def setmlbb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        await update.message.reply_text("This command can only be used in private chat.")
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /setmlbb <mlbb_id>")
        return

    mlbb_id = context.args[0].strip()
    if not mlbb_id:
        await update.message.reply_text("Usage: /setmlbb <mlbb_id>")
        return

    user_id = update.effective_user.id
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, mlbb_id)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET mlbb_id = EXCLUDED.mlbb_id
                """,
                (user_id, mlbb_id),
            )

    await update.message.reply_text("MLBB ID saved ✅")


async def challenge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.message.reply_text("This command can only be used in group chat.")
        return

    if len(context.args) != 2:
        await update.message.reply_text("Usage: /challenge <amount> <game>")
        return

    amount_raw = context.args[0]
    game = context.args[1].lower()

    try:
        amount = int(amount_raw)
    except ValueError:
        await update.message.reply_text("Invalid amount. Usage: /challenge <amount> <game>")
        return

    if amount <= 0:
        await update.message.reply_text("Invalid amount. Usage: /challenge <amount> <game>")
        return

    if game not in {"dice", "mlbb", "chess"}:
        await update.message.reply_text("Invalid game. Use one of: dice, mlbb, chess")
        return

    player1 = update.effective_user.id

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO matches (player1, player2, game, amount, status, result1, result2)
                VALUES (%s, NULL, %s, %s, 'waiting', NULL, NULL)
                RETURNING match_id
                """,
                (player1, game, amount),
            )
            match_id = cur.fetchone()["match_id"]

    await update.message.reply_text(
        "Challenge created!\n"
        f"Match ID: {match_id}\n"
        f"Game: {game}\n"
        f"Amount: {amount}\n"
        "Use /accept <id> to join"
    )


async def accept_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.message.reply_text("This command can only be used in group chat.")
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /accept <match_id>")
        return

    try:
        match_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid match_id.")
        return

    accepter_id = update.effective_user.id

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM matches WHERE match_id = %s", (match_id,))
            match = cur.fetchone()

            if not match:
                await update.message.reply_text("Invalid match_id.")
                return

            if match["status"] != "waiting":
                await update.message.reply_text("This match is not available for acceptance.")
                return

            if match["player1"] == accepter_id:
                await update.message.reply_text("You cannot accept your own challenge.")
                return

            cur.execute(
                "UPDATE matches SET player2 = %s, status = 'active' WHERE match_id = %s",
                (accepter_id, match_id),
            )

            player1_id = match["player1"]
            player2_id = accepter_id
            game = match["game"]

            if game == "dice":
                roll1 = random.randint(1, 6)
                roll2 = random.randint(1, 6)

                if roll1 > roll2:
                    winner_id = player1_id
                elif roll2 > roll1:
                    winner_id = player2_id
                else:
                    winner_id = None

                cur.execute("UPDATE matches SET status = 'completed' WHERE match_id = %s", (match_id,))

                if winner_id is None:
                    winner_text = "Winner: Draw"
                else:
                    winner_text = f"Winner: {user_label(winner_id)}"

                await update.message.reply_text(
                    f"Player1 roll: {roll1}\n"
                    f"Player2 roll: {roll2}\n"
                    f"{winner_text}"
                )
                return

            if game == "chess":
                p1_name = await group_user_display(update, player1_id)
                p2_name = await group_user_display(update, player2_id)
                await update.message.reply_text(
                    "Play chess here: https://lichess.org/\n"
                    f"Player1: {p1_name}\n"
                    f"Player2: {p2_name}\n"
                    "After playing, submit result using /result <win/lose>"
                )
                return

            cur.execute("SELECT mlbb_id FROM users WHERE user_id = %s", (player1_id,))
            p1_row = cur.fetchone()
            cur.execute("SELECT mlbb_id FROM users WHERE user_id = %s", (player2_id,))
            p2_row = cur.fetchone()

            p1_mlbb = p1_row["mlbb_id"] if p1_row else None
            p2_mlbb = p2_row["mlbb_id"] if p2_row else None

            if not p1_mlbb or not p2_mlbb:
                cur.execute("UPDATE matches SET status = 'cancelled' WHERE match_id = %s", (match_id,))
                await update.message.reply_text("Both players must set MLBB ID first. Match cancelled.")
                return

            await update.message.reply_text(
                f"Player1 MLBB ID: {p1_mlbb}\n"
                f"Player2 MLBB ID: {p2_mlbb}\n"
                "Play your MLBB match manually, then submit /result <win/lose>"
            )


def get_latest_active_manual_match(conn: psycopg.Connection, user_id: int) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM matches
            WHERE status = 'active'
              AND game IN ('chess', 'mlbb')
              AND (player1 = %s OR player2 = %s)
            ORDER BY match_id DESC
            LIMIT 1
            """,
            (user_id, user_id),
        )
        return cur.fetchone()


async def result_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.message.reply_text("This command can only be used in group chat.")
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /result <win/lose>")
        return

    user_result = context.args[0].lower()
    if user_result not in {"win", "lose"}:
        await update.message.reply_text("Usage: /result <win/lose>")
        return

    user_id = update.effective_user.id

    with get_db_connection() as conn:
        match = get_latest_active_manual_match(conn, user_id)
        if not match:
            await update.message.reply_text("No active chess/mlbb match found for you.")
            return

        match_id = match["match_id"]

        with conn.cursor() as cur:
            if match["player1"] == user_id:
                cur.execute("UPDATE matches SET result1 = %s WHERE match_id = %s", (user_result, match_id))
            elif match["player2"] == user_id:
                cur.execute("UPDATE matches SET result2 = %s WHERE match_id = %s", (user_result, match_id))
            else:
                await update.message.reply_text("You are not part of this match.")
                return

            cur.execute("SELECT * FROM matches WHERE match_id = %s", (match_id,))
            updated = cur.fetchone()

            result1 = updated["result1"]
            result2 = updated["result2"]

            if not result1 or not result2:
                await update.message.reply_text("Result submitted. Waiting for the other player.")
                return

            if result1 == result2:
                cur.execute("UPDATE matches SET status = 'dispute' WHERE match_id = %s", (match_id,))
                await update.message.reply_text("Dispute detected. Admin will decide.")
                return

            if result1 == "win" and result2 == "lose":
                winner_id = updated["player1"]
            elif result1 == "lose" and result2 == "win":
                winner_id = updated["player2"]
            else:
                cur.execute("UPDATE matches SET status = 'dispute' WHERE match_id = %s", (match_id,))
                await update.message.reply_text("Dispute detected. Admin will decide.")
                return

            cur.execute("UPDATE matches SET status = 'completed' WHERE match_id = %s", (match_id,))

    await update.message.reply_text(f"Match completed! Winner: {user_label(winner_id)}")


async def resolve_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.message.reply_text("This command can only be used in group chat.")
        return

    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Unauthorized.")
        return

    if len(context.args) != 2:
        await update.message.reply_text("Usage: /resolve <match_id> <winner_user_id>")
        return

    try:
        match_id = int(context.args[0])
        winner_user_id = int(context.args[1])
    except ValueError:
        await update.message.reply_text("Invalid arguments. Usage: /resolve <match_id> <winner_user_id>")
        return

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT match_id FROM matches WHERE match_id = %s", (match_id,))
            row = cur.fetchone()
            if not row:
                await update.message.reply_text("Invalid match_id.")
                return

            cur.execute("UPDATE matches SET status = 'completed' WHERE match_id = %s", (match_id,))

    await update.message.reply_text(f"Admin resolved match {match_id}. Winner: {user_label(winner_user_id)}")


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing. Set BOT_TOKEN environment variable.")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("setmlbb", setmlbb_command))
    app.add_handler(CommandHandler("challenge", challenge_command))
    app.add_handler(CommandHandler("accept", accept_command))
    app.add_handler(CommandHandler("result", result_command))
    app.add_handler(CommandHandler("resolve", resolve_command))
    
    if WEBHOOK_URL:
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=f"{WEBHOOK_URL.rstrip('/')}/{BOT_TOKEN}",
        )
    else:
        app.run_polling()

if __name__ == "__main__":
    main()
