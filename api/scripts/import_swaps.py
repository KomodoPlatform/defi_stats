#!/usr/bin/env python3
import os
import sys
from datetime import date
API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)
from const import RESET_TABLE
from db import SqlSource


if __name__ == "__main__":
    if RESET_TABLE:
        DB = SqlSource()
        DB.reset_defi_stats_table()
    DB.import_swaps(start_dt=date(2019, 1, 15), end_dt=date(2024, 2, 3))


# DB Table validation:
# - Pair format is BASE_QUOTE                                                |

# - If "maker_coin" is BASE, "trade_type" is "buy"                           | MAKER_TAKER == 'buy'
# - if "trade_type" is "buy",  "price" is ("taker_amount" / "maker_amount")  | 1 LTC / 100 KMD = 0.01

# - If "taker_coin" is BASE, "trade_type" is "sell"                          | TAKER_MAKER == 'sell'
# - if "trade_type" is "sell", "price" is ("maker_amount" / "taker_amount")  | 1 LTC / 100 KMD = 0.01
