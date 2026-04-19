from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo


def main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
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
    if is_admin:
        rows.append([InlineKeyboardButton("Admin Panel", callback_data="menu:admin")])
    return InlineKeyboardMarkup(rows)


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


def challenge_card_keyboard(match_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Accept Match", callback_data=f"accept:{match_id}"),
                InlineKeyboardButton("Cancel", callback_data=f"cancel:{match_id}"),
            ]
        ]
    )


def dice_roll_keyboard(match_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Roll Dice", callback_data=f"dice_roll:{match_id}")]]
    )


def football_roll_keyboard(match_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Take Shot", callback_data=f"football_roll:{match_id}")]]
    )


def dice_reroll_keyboard(match_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Roll Again", callback_data=f"dice_roll:{match_id}")]]
    )


def football_reroll_keyboard(match_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Shoot Again", callback_data=f"football_roll:{match_id}")]]
    )


def mlbb_result_keyboard(match_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("I Won", callback_data=f"mlbb_result:{match_id}:win"),
                InlineKeyboardButton("I Lost", callback_data=f"mlbb_result:{match_id}:lose"),
            ]
        ]
    )


def chess_keyboard(match_id: str, user_id: int | str, webhook_url: str) -> InlineKeyboardMarkup:
    url = f"{webhook_url.rstrip('/')}/chess?match_id={match_id}&user_id={user_id}"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Open Chess Board", web_app=WebAppInfo(url=url))]]
    )


def accept_challenge_keyboard(match_id: str) -> InlineKeyboardMarkup:
    return challenge_card_keyboard(match_id)


def withdrawal_admin_keyboard(withdrawal_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Approve", callback_data=f"admin_withdraw_approve:{withdrawal_id}"),
                InlineKeyboardButton("Reject", callback_data=f"admin_withdraw_reject:{withdrawal_id}"),
            ]
        ]
    )
