from typing import Optional
from datetime import datetime
from decimal import Decimal
from sqlalchemy.ext.hybrid import hybrid_property
from sqlmodel import Field, SQLModel
from util.enums import TradeType



class DefiSwap(SQLModel, table=True):
    __tablename__ = "defi_swaps"

    id: Optional[int] = Field(default=None, primary_key=True)
    uuid: str = Field(
        default="77777777-7777-7777-7777-777777777777", unique=True, nullable=False
    )
    pair: str = "XXX-PROTO_YYY-PROTO"
    trade_type: TradeType = "ALL"
    is_success: int = 0
    taker_amount: Decimal = 777.777777
    taker_coin: str = "XXX-PROTO"
    taker_coin_ticker: str = "XXX"
    taker_coin_platform: str = "PROTO"
    taker_gui: str = "unknown"
    taker_pubkey: str = "unknown"
    taker_version: str = "unknown"
    taker_coin_usd_price: Decimal = 0.00
    maker_amount: Decimal = 777.777777
    maker_coin: str = "YYY-PROTO"
    maker_coin_ticker: str = "YYY"
    maker_coin_platform: str = "PROTO"
    maker_gui: str = "unknown"
    maker_pubkey: str = "unknown"
    maker_version: str = "unknown"
    maker_coin_usd_price: Decimal = 0.00
    price: Decimal = 777.777777
    reverse_price: Decimal = 777.777777
    started_at: int = 0
    finished_at: int = 0
    duration: int = 0
    validated: bool = False
    last_updated: int = 0


class CipiSwap(SQLModel, table=True):
    __tablename__ = "swaps"
    id: Optional[int] = Field(default=None, primary_key=True)
    started_at: datetime = 1777777777
    uuid: str = "77777777-7777-7777-7777-777777777777"
    taker_amount: Decimal = 777.777777
    taker_coin: str = ""
    taker_gui: str = "unknown"
    taker_pubkey: str = "unknown"
    taker_version: str = "unknown"
    maker_amount: Decimal = 777.777777
    maker_coin: str = ""
    maker_gui: str = "unknown"
    maker_pubkey: str = "unknown"
    maker_version: str = "unknown"


class StatsSwap(SQLModel, table=True):
    __tablename__ = "stats_swaps"
    id: Optional[int] = Field(default=None, primary_key=True)
    maker_coin: str = ""
    taker_coin: str = ""
    uuid: str = "77777777-7777-7777-7777-777777777777"
    started_at: int = 1777777777
    finished_at: int = 1777777777
    maker_amount: Decimal = 777.777777
    taker_amount: Decimal = 777.777777
    is_success: int = 0
    maker_coin_ticker: str = ""
    maker_coin_platform: str = ""
    taker_coin_ticker: str = ""
    taker_coin_platform: str = ""
    maker_coin_usd_price: Decimal = 0.00
    taker_coin_usd_price: Decimal = 0.00
    taker_pubkey: str = "unknown"
    maker_pubkey: str = "unknown"
