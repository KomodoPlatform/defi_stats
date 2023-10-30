
from enum import Enum


class TradeType(str, Enum):
    BUY = "buy"
    SELL = "sell"
    ALL = "all"
