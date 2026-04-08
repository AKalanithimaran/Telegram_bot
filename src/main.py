import json
import math
import os
import random
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import chess
import psycopg
import uvicorn
from dotenv import load_dotenv
from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from psycopg.rows import dict_row
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
PORT = int(os.getenv("PORT", "10000"))
HTML_PATH = Path(__file__).with_name("chess.html")

try:
    ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "6204931777").strip())
except ValueError:
    ADMIN_USER_ID = 6204931777

telegram_app: Optional[Application] = None
polling_started = False


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
            cur.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS winner BIGINT")
            cur.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS chess_token TEXT")
            cur.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS chess_state TEXT")
            cur.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS chat_id BIGINT")


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


def build_match_link(match_token: str, seat: str, auth: str) -> str:
    if not WEBHOOK_URL:
        return ""
    base = WEBHOOK_URL.rstrip("/")
    return f"{base}/chess/{match_token}?seat={seat}&auth={auth}"


def build_chess_state(match_id: int, player1_id: int, player2_id: int, player1_name: str, player2_name: str) -> tuple[str, dict[str, Any]]:
    token = secrets.token_urlsafe(18)
    state = {
        "match_id": match_id,
        "status": "lobby",
        "time_control": None,
        "last_clock_update": None,
        "turn": "white",
        "fen": chess.STARTING_FEN,
        "winner": None,
        "result_reason": None,
        "white": {
            "id": player1_id,
            "name": player1_name,
            "joined": False,
            "time_left": None,
            "auth": secrets.token_urlsafe(12),
        },
        "black": {
            "id": player2_id,
            "name": player2_name,
            "joined": False,
            "time_left": None,
            "auth": secrets.token_urlsafe(12),
        },
        "messages": ["Choose a time control to start the match."],
    }
    return token, state


def load_chess_state(raw_state: Optional[str]) -> dict[str, Any]:
    if not raw_state:
        raise HTTPException(status_code=404, detail="Chess match state not found.")
    return json.loads(raw_state)


def save_chess_state(cur: psycopg.Cursor, match_id: int, state: dict[str, Any]) -> None:
    cur.execute(
        "UPDATE matches SET chess_state = %s WHERE match_id = %s",
        (json.dumps(state), match_id),
    )


def get_seat_data(state: dict[str, Any], seat: str) -> dict[str, Any]:
    if seat not in {"white", "black"}:
        raise HTTPException(status_code=400, detail="Invalid seat.")
    return state[seat]


def require_chess_auth(state: dict[str, Any], seat: str, auth: str) -> dict[str, Any]:
    seat_data = get_seat_data(state, seat)
    if seat_data["auth"] != auth:
        raise HTTPException(status_code=403, detail="Invalid match link.")
    return seat_data


def board_from_state(state: dict[str, Any]) -> chess.Board:
    return chess.Board(state["fen"])


def clock_snapshot(seconds: Optional[float]) -> Optional[int]:
    if seconds is None:
        return None
    return max(0, int(math.ceil(seconds)))


def legal_moves_map(board: chess.Board) -> dict[str, list[str]]:
    moves: dict[str, list[str]] = {}
    for move in board.legal_moves:
        from_square = chess.square_name(move.from_square)
        to_square = chess.square_name(move.to_square)
        moves.setdefault(from_square, [])
        if move.promotion:
            suffix = chess.piece_symbol(move.promotion)
            moves[from_square].append(f"{to_square}:{suffix}")
        else:
            moves[from_square].append(to_square)
    return moves


async def announce_winner(match: dict[str, Any], winner_id: Optional[int], text: str) -> None:
    if not telegram_app:
        return
    chat_id = match.get("chat_id")
    if not chat_id:
        return
    await telegram_app.bot.send_message(chat_id=chat_id, text=text)


async def complete_match(match_id: int, winner_id: Optional[int], status: str, state: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            chess_state_json = json.dumps(state) if state is not None else None
            if chess_state_json is None:
                cur.execute(
                    "UPDATE matches SET status = %s, winner = %s WHERE match_id = %s RETURNING *",
                    (status, winner_id, match_id),
                )
            else:
                cur.execute(
                    "UPDATE matches SET status = %s, winner = %s, chess_state = %s WHERE match_id = %s RETURNING *",
                    (status, winner_id, chess_state_json, match_id),
                )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Match not found.")
            return row


async def resolve_chess_timeout(match: dict[str, Any], state: dict[str, Any], now: Optional[float] = None) -> tuple[dict[str, Any], bool]:
    if state["status"] != "active" or not state["time_control"] or state["last_clock_update"] is None:
        return state, False

    now = now or time.time()
    elapsed = max(0.0, now - float(state["last_clock_update"]))
    turn = state["turn"]
    seat_state = state[turn]
    seat_state["time_left"] = max(0.0, float(seat_state["time_left"]) - elapsed)
    state["last_clock_update"] = now

    if seat_state["time_left"] <= 0:
        winner_seat = "black" if turn == "white" else "white"
        state["status"] = "completed"
        state["winner"] = state[winner_seat]["id"]
        state["result_reason"] = "timeout"
        completed_match = await complete_match(match["match_id"], state[winner_seat]["id"], "completed", state)
        await announce_winner(
            completed_match,
            state[winner_seat]["id"],
            f"♟️ Chess Match Finished!\n\n🏆 Winner: {state[winner_seat]['name']}\nReason: Timeout",
        )
        return state, True

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            save_chess_state(cur, match["match_id"], state)
    return state, False


def serialize_chess_view(match: dict[str, Any], state: dict[str, Any], seat: str) -> dict[str, Any]:
    board = board_from_state(state)
    viewer = get_seat_data(state, seat)
    return {
        "match_id": match["match_id"],
        "status": state["status"],
        "time_control": state["time_control"],
        "turn": state["turn"],
        "fen": state["fen"],
        "winner": state["winner"],
        "result_reason": state["result_reason"],
        "viewer_seat": seat,
        "viewer_id": viewer["id"],
        "white": {
            "id": state["white"]["id"],
            "name": state["white"]["name"],
            "joined": state["white"]["joined"],
            "time_left": clock_snapshot(state["white"]["time_left"]),
        },
        "black": {
            "id": state["black"]["id"],
            "name": state["black"]["name"],
            "joined": state["black"]["joined"],
            "time_left": clock_snapshot(state["black"]["time_left"]),
        },
        "messages": state.get("messages", []),
        "legal_moves": legal_moves_map(board) if state["status"] == "active" else {},
        "piece_map": {chess.square_name(square): piece.symbol() for square, piece in board.piece_map().items()},
    }


def get_manual_mlbb_match(conn: psycopg.Connection, user_id: int) -> Optional[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM matches
            WHERE status = 'active'
              AND game = 'mlbb'
              AND (player1 = %s OR player2 = %s)
            ORDER BY match_id DESC
            LIMIT 1
            """,
            (user_id, user_id),
        )
        return cur.fetchone()


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

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, mlbb_id)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET mlbb_id = EXCLUDED.mlbb_id
                """,
                (update.effective_user.id, mlbb_id),
            )

    await update.message.reply_text("MLBB ID saved ✅")


async def challenge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.message.reply_text("This command can only be used in group chat.")
        return

    if len(context.args) != 2:
        await update.message.reply_text("Usage: /challenge <amount> <game>")
        return

    try:
        amount = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid amount. Usage: /challenge <amount> <game>")
        return

    game = context.args[1].strip().lower()
    if amount <= 0:
        await update.message.reply_text("Invalid amount. Usage: /challenge <amount> <game>")
        return

    if game not in {"dice", "chess", "mlbb"}:
        await update.message.reply_text("Invalid game. Use one of: dice, chess, mlbb")
        return

    player_name = await group_user_display(update, update.effective_user.id)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO matches (player1, player2, game, amount, status, result1, result2, chat_id)
                VALUES (%s, NULL, %s, %s, 'waiting', NULL, NULL, %s)
                RETURNING match_id
                """,
                (update.effective_user.id, game, amount, update.effective_chat.id),
            )
            match_id = cur.fetchone()["match_id"]

    await update.message.reply_text(
        "🎮 PvP Challenge!\n\n"
        f"{player_name} has challenged!\n\n"
        f"💰 Bet: {amount}\n"
        f"🎯 Game: {game}\n\n"
        "Waiting for opponent...\n\n"
        f"Use /accept {match_id}"
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

            player1_name = await group_user_display(update, player1_id)
            player2_name = await group_user_display(update, player2_id)

            if game == "dice":
                rounds: list[tuple[int, int]] = []
                while True:
                    roll1 = random.randint(1, 6)
                    roll2 = random.randint(1, 6)
                    rounds.append((roll1, roll2))
                    if roll1 != roll2:
                        break

                winner_name = player1_name if rounds[-1][0] > rounds[-1][1] else player2_name
                winner_id = player1_id if rounds[-1][0] > rounds[-1][1] else player2_id
                cur.execute(
                    "UPDATE matches SET status = 'completed', winner = %s WHERE match_id = %s",
                    (winner_id, match_id),
                )

                round_lines = []
                for index, (p1_roll, p2_roll) in enumerate(rounds, start=1):
                    label = "" if len(rounds) == 1 else f"Round {index}\n"
                    round_lines.append(
                        f"{label}{player1_name} rolled: 🎲 {p1_roll}\n{player2_name} rolled: 🎲 {p2_roll}"
                    )

                await update.message.reply_text(
                    "🎲 Dice Match Started!\n\n"
                    + "\n\n".join(round_lines)
                    + f"\n\n🏆 Winner: {winner_name}"
                )
                return

            if game == "chess":
                if not WEBHOOK_URL:
                    cur.execute("UPDATE matches SET status = 'cancelled' WHERE match_id = %s", (match_id,))
                    await update.message.reply_text("Chess match cannot start because WEBHOOK_URL is not configured.")
                    return

                chess_token, chess_state = build_chess_state(match_id, player1_id, player2_id, player1_name, player2_name)
                cur.execute(
                    "UPDATE matches SET chess_token = %s, chess_state = %s WHERE match_id = %s",
                    (json.dumps(chess_token)[1:-1], json.dumps(chess_state), match_id),
                )
                white_link = build_match_link(chess_token, "white", chess_state["white"]["auth"])
                black_link = build_match_link(chess_token, "black", chess_state["black"]["auth"])

                await update.message.reply_text(
                    "♟️ Chess Match Started!\n\n"
                    f"{player1_name} vs {player2_name}\n\n"
                    "Open your page, choose time, and play:\n\n"
                    f"White ({player1_name}): {white_link}\n\n"
                    f"Black ({player2_name}): {black_link}"
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
                await update.message.reply_text("Both users must set MLBB ID first.")
                return

            await update.message.reply_text(
                "🎮 MLBB Match Started!\n\n"
                f"{player1_name} ID: {p1_mlbb}\n"
                f"{player2_name} ID: {p2_mlbb}\n\n"
                "Add each other and start the match!"
            )


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
        match = get_manual_mlbb_match(conn, user_id)
        if not match:
            await update.message.reply_text("No active MLBB match found for you.")
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
                await update.message.reply_text("⚠️ Dispute detected. Admin will decide.")
                return

            if result1 == "win" and result2 == "lose":
                winner_id = updated["player1"]
            elif result1 == "lose" and result2 == "win":
                winner_id = updated["player2"]
            else:
                cur.execute("UPDATE matches SET status = 'dispute' WHERE match_id = %s", (match_id,))
                await update.message.reply_text("⚠️ Dispute detected. Admin will decide.")
                return

            cur.execute(
                "UPDATE matches SET status = 'completed', winner = %s WHERE match_id = %s",
                (winner_id, match_id),
            )

    winner_name = await group_user_display(update, winner_id)
    await update.message.reply_text(f"🏆 Winner: {winner_name}")


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
            cur.execute("SELECT * FROM matches WHERE match_id = %s", (match_id,))
            match = cur.fetchone()
            if not match:
                await update.message.reply_text("Invalid match_id.")
                return

            if winner_user_id not in {match["player1"], match["player2"]}:
                await update.message.reply_text("Winner must be one of the match players.")
                return

            chess_state = load_chess_state(match["chess_state"]) if match.get("chess_state") else None
            if chess_state:
                chess_state["status"] = "completed"
                chess_state["winner"] = winner_user_id
                chess_state["result_reason"] = "admin_resolved"
                cur.execute(
                    "UPDATE matches SET status = 'completed', winner = %s, chess_state = %s WHERE match_id = %s",
                    (winner_user_id, json.dumps(chess_state), match_id),
                )
            else:
                cur.execute(
                    "UPDATE matches SET status = 'completed', winner = %s WHERE match_id = %s",
                    (winner_user_id, match_id),
                )

    winner_name = await group_user_display(update, winner_user_id)
    await update.message.reply_text(f"🏆 Winner: {winner_name}")


def build_telegram_application() -> Application:
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("setmlbb", setmlbb_command))
    application.add_handler(CommandHandler("challenge", challenge_command))
    application.add_handler(CommandHandler("accept", accept_command))
    application.add_handler(CommandHandler("result", result_command))
    application.add_handler(CommandHandler("resolve", resolve_command))
    return application


@asynccontextmanager
async def lifespan(_: FastAPI):
    global telegram_app, polling_started

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing. Set BOT_TOKEN environment variable.")

    print("Initializing database...")
    init_db()
    print("Database initialized.")

    telegram_app = build_telegram_application()
    await telegram_app.initialize()
    await telegram_app.start()

    if WEBHOOK_URL:
        webhook_target = f"{WEBHOOK_URL.rstrip('/')}/{BOT_TOKEN}"
        await telegram_app.bot.set_webhook(url=webhook_target)
        print(f"Webhook set to {webhook_target}")
    else:
        if telegram_app.updater is None:
            raise RuntimeError("Telegram updater is unavailable.")
        await telegram_app.updater.start_polling(drop_pending_updates=True)
        polling_started = True
        print("Telegram polling started.")

    try:
        yield
    finally:
        if telegram_app:
            if WEBHOOK_URL:
                await telegram_app.bot.delete_webhook(drop_pending_updates=False)
            if polling_started and telegram_app.updater is not None:
                await telegram_app.updater.stop()
            await telegram_app.stop()
            await telegram_app.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=PlainTextResponse)
async def home() -> str:
    return "PvP Bot is running"


@app.post(f"/{BOT_TOKEN}")
async def telegram_webhook(request: Request) -> JSONResponse:
    if not telegram_app:
        raise HTTPException(status_code=503, detail="Telegram app is not ready.")
    data = await request.json()
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return JSONResponse({"ok": True})


@app.get("/chess/{token}", response_class=HTMLResponse)
async def chess_page(token: str) -> HTMLResponse:
    if not HTML_PATH.exists():
        raise HTTPException(status_code=404, detail="Chess page not found.")
    html = HTML_PATH.read_text(encoding="utf-8")
    html = html.replace("__MATCH_TOKEN__", token)
    return HTMLResponse(html)


@app.post("/api/chess/{token}/join")
async def chess_join(token: str, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    seat = str(payload.get("seat", ""))
    auth = str(payload.get("auth", ""))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM matches WHERE chess_token = %s", (token,))
            match = cur.fetchone()
            if not match:
                raise HTTPException(status_code=404, detail="Match not found.")

            state = load_chess_state(match["chess_state"])
            seat_state = require_chess_auth(state, seat, auth)
            seat_state["joined"] = True
            if state["time_control"] and state["white"]["joined"] and state["black"]["joined"] and state["status"] == "lobby":
                state["status"] = "active"
                state["white"]["time_left"] = float(state["time_control"])
                state["black"]["time_left"] = float(state["time_control"])
                state["last_clock_update"] = time.time()
                state["messages"] = [f"{state['white']['name']} vs {state['black']['name']} started."]

            save_chess_state(cur, match["match_id"], state)

    return JSONResponse({"ok": True, "player": seat_state["name"]})


@app.post("/api/chess/{token}/time")
async def chess_time(token: str, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    seat = str(payload.get("seat", ""))
    auth = str(payload.get("auth", ""))
    seconds = int(payload.get("seconds", 0))
    if seconds not in {180, 300, 600}:
        raise HTTPException(status_code=400, detail="Invalid time control.")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM matches WHERE chess_token = %s", (token,))
            match = cur.fetchone()
            if not match:
                raise HTTPException(status_code=404, detail="Match not found.")

            state = load_chess_state(match["chess_state"])
            require_chess_auth(state, seat, auth)
            if state["status"] != "lobby":
                raise HTTPException(status_code=400, detail="Time control already locked.")
            if state["time_control"] is not None:
                raise HTTPException(status_code=400, detail="Time control already selected.")

            state["time_control"] = seconds
            state["messages"] = [f"Time control set to {seconds // 60} min."]
            if state["white"]["joined"] and state["black"]["joined"]:
                state["status"] = "active"
                state["white"]["time_left"] = float(seconds)
                state["black"]["time_left"] = float(seconds)
                state["last_clock_update"] = time.time()
                state["messages"].append("Both players joined. Match started.")

            save_chess_state(cur, match["match_id"], state)

    return JSONResponse({"ok": True})


@app.get("/api/chess/{token}/state")
async def chess_state(token: str, seat: str, auth: str) -> JSONResponse:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM matches WHERE chess_token = %s", (token,))
            match = cur.fetchone()
            if not match:
                raise HTTPException(status_code=404, detail="Match not found.")

    state = load_chess_state(match["chess_state"])
    require_chess_auth(state, seat, auth)
    state, _ = await resolve_chess_timeout(match, state)
    return JSONResponse(serialize_chess_view(match, state, seat))


@app.post("/api/chess/{token}/move")
async def chess_move(token: str, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    seat = str(payload.get("seat", ""))
    auth = str(payload.get("auth", ""))
    from_square = str(payload.get("from", ""))
    to_square = str(payload.get("to", ""))
    promotion = str(payload.get("promotion", "q"))[:1].lower()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM matches WHERE chess_token = %s", (token,))
            match = cur.fetchone()
            if not match:
                raise HTTPException(status_code=404, detail="Match not found.")

    state = load_chess_state(match["chess_state"])
    require_chess_auth(state, seat, auth)

    if state["status"] != "active":
        raise HTTPException(status_code=400, detail="Match is not active.")
    if state["turn"] != seat:
        raise HTTPException(status_code=400, detail="Not your turn.")

    state, timed_out = await resolve_chess_timeout(match, state)
    if timed_out:
        return JSONResponse(serialize_chess_view(match, state, seat))

    board = board_from_state(state)
    uci = f"{from_square}{to_square}"
    if promotion in {"q", "r", "b", "n"} and (len(from_square) == 2 and len(to_square) == 2):
        maybe_promo = f"{uci}{promotion}"
        try:
            move = chess.Move.from_uci(maybe_promo)
            if move in board.legal_moves:
                uci = maybe_promo
        except ValueError:
            pass

    try:
        move = chess.Move.from_uci(uci)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid move.") from exc

    if move not in board.legal_moves:
        raise HTTPException(status_code=400, detail="Illegal move.")

    board.push(move)
    state["fen"] = board.fen()
    state["turn"] = "black" if seat == "white" else "white"
    state["last_clock_update"] = time.time()
    state["messages"] = [f"{state[seat]['name']} played {board.peek().uci()}."]

    winner_id: Optional[int] = None
    announcement: Optional[str] = None
    if board.is_checkmate():
        state["status"] = "completed"
        state["winner"] = state[seat]["id"]
        state["result_reason"] = "checkmate"
        winner_id = state[seat]["id"]
        announcement = f"♟️ Chess Match Finished!\n\n🏆 Winner: {state[seat]['name']}\nReason: Checkmate"
    elif board.is_stalemate() or board.is_insufficient_material() or board.can_claim_threefold_repetition():
        state["status"] = "completed"
        state["winner"] = None
        state["result_reason"] = "draw"
        announcement = "♟️ Chess Match Finished!\n\nResult: Draw"

    completed_match: Optional[dict[str, Any]] = None
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if state["status"] == "completed":
                cur.execute(
                    "UPDATE matches SET status = 'completed', winner = %s, chess_state = %s WHERE match_id = %s RETURNING *",
                    (winner_id, json.dumps(state), match["match_id"]),
                )
                completed_match = cur.fetchone()
            else:
                save_chess_state(cur, match["match_id"], state)

    if completed_match and announcement:
        await announce_winner(completed_match, winner_id, announcement)

    return JSONResponse(serialize_chess_view(match, state, seat))


@app.post("/api/chess/{token}/resign")
async def chess_resign(token: str, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    seat = str(payload.get("seat", ""))
    auth = str(payload.get("auth", ""))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM matches WHERE chess_token = %s", (token,))
            match = cur.fetchone()
            if not match:
                raise HTTPException(status_code=404, detail="Match not found.")
            state = load_chess_state(match["chess_state"])
            require_chess_auth(state, seat, auth)
            if state["status"] != "active":
                raise HTTPException(status_code=400, detail="Match is not active.")

            winner_seat = "black" if seat == "white" else "white"
            state["status"] = "completed"
            state["winner"] = state[winner_seat]["id"]
            state["result_reason"] = "resignation"
            cur.execute(
                "UPDATE matches SET status = 'completed', winner = %s, chess_state = %s WHERE match_id = %s RETURNING *",
                (state[winner_seat]["id"], json.dumps(state), match["match_id"]),
            )
            completed_match = cur.fetchone()

    await announce_winner(
        completed_match,
        state[winner_seat]["id"],
        f"♟️ Chess Match Finished!\n\n🏆 Winner: {state[winner_seat]['name']}\nReason: Resignation",
    )
    return JSONResponse({"ok": True})


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
