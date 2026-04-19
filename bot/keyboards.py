from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Balance", callback_data="menu:balance"),
                InlineKeyboardButton("Games", callback_data="menu:games"),
            ],
            [
                InlineKeyboardButton("Deposit", callback_data="menu:deposit"),
                InlineKeyboardButton("Withdraw", callback_data="menu:withdraw"),
            ],
            [
                InlineKeyboardButton("Profile", callback_data="menu:profile"),
                InlineKeyboardButton("History", callback_data="menu:history"),
            ],
            [
                InlineKeyboardButton("Leaderboard", callback_data="menu:leaderboard"),
                InlineKeyboardButton("Tip", callback_data="menu:tip"),
            ],
        ]
    )


def deposit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("TON", callback_data="deposit:TON")],
            [InlineKeyboardButton("USDT (BEP20)", callback_data="deposit:USDT_BEP20")],
            [InlineKeyboardButton("SOL", callback_data="deposit:SOL")],
        ]
    )


def games_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Dice", callback_data="games:dice")],
            [InlineKeyboardButton("Football", callback_data="games:football")],
            [InlineKeyboardButton("Chess", callback_data="games:chess")],
            [InlineKeyboardButton("MLBB", callback_data="games:mlbb")],
        ]
    )


def accept_challenge_keyboard(match_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Accept Challenge", callback_data=f"accept:{match_id}")]])
