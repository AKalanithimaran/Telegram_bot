# Telegram Gambling Bot

Webhook-based Telegram gambling bot built with Python, `python-telegram-bot` 20.8, MongoDB, and Render-friendly Starlette hosting.

## Highlights

- Webhook-only bot runtime
- MongoDB via `motor` and `pymongo`
- TON auto-deposit polling every 30 seconds
- Dice, football, chess, and MLBB wager flows
- Admin moderation, balance tools, VIP leaderboard, and house accounting
- Role-based runtime split with `APP_ROLE=web|worker`
- Readiness endpoint: `GET /health/ready`

## Run Locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

## GIF Configuration (Dice / Football)

Add these optional environment variables to send Telegram GIF animations:

- `DICE_GIF_FILE_ID`
- `FOOTBALL_GIF_FILE_ID`

If unset, the bot continues normal gameplay without GIF messages.

## Important Endpoints

- `GET /health`
- `GET /health/ready`
- `POST /webhook`
- `GET /chess?match_id=...&user_id=...`
- `POST /chess_result`
