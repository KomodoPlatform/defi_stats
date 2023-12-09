from enum import Enum


class TradeType(str, Enum):
    BUY = "buy"
    SELL = "sell"
    ALL = "all"


class NetId(str, Enum):
    NETID_7777 = "7777"
    NETID_8762 = "8762"
    ALL = "all"
