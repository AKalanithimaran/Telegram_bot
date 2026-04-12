# Telegram PvP Bot

Production-oriented layout for the Telegram PvP bot.

## Structure

```text
src/
  main.py              # thin compatibility entrypoint
  pvp_bot/
    __init__.py
    __main__.py        # package entrypoint
    app.py             # application bootstrap
    config.py          # environment and logging
    database.py        # SQLite schema and persistence helpers
    handlers.py        # Telegram command handlers
    jobs.py            # background jobs
    match_service.py   # payout, dispute, and match business logic
    telegram_helpers.py
    ton.py             # TonCenter integration
    utils.py
    assets/
      chess.html
data/                  # default SQLite location
render.yaml
requirements.txt
.env.example
```

## Local Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
PYTHONPATH=src python3 -m pvp_bot
```

## Render

The repo is configured as a worker service and starts with:

```bash
PYTHONPATH=src python3 -m pvp_bot
```
