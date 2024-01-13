from typing import Optional
from decimal import Decimal
from datetime import datetime

from sqlmodel import Field, Session, SQLModel, create_engine

class Swaps(SQLModel, table=True):
    __tablename__ = "base_swaps"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    uuid: str = "77777777-7777-7777-7777-777777777777"
    taker_amount: Decimal = 777.777777
    taker_coin: str = ""
    taker_coin_platform: str = ""
    taker_coin_ticker: str = ""
    taker_gui: str = "unknown"
    taker_pubkey: str = "unknown"
    taker_version: str = "unknown"
    maker_amount: Decimal = 777.777777
    maker_coin: str = ""
    maker_coin_platform: str = ""
    maker_coin_ticker: str = ""
    maker_gui: str = "unknown"
    maker_pubkey: str = "unknown"
    maker_version: str = "unknown"

class StatsSwaps(Swaps):
    __tablename__ = "stats_swaps"    
    started_at: int = 1777777777
    finished_at: int = 1777777777
    maker_coin_usd_price: Decimal = 777.777777
    taker_coin_usd_price: Decimal = 777.777777
    price: Decimal = 777.777777
    reverse_price: Decimal = 777.777777
    is_success: str = 0

class CipiSwaps(SQLModel, table=True):
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
    