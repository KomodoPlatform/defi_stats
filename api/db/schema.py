from typing import Optional
from decimal import Decimal

from sqlmodel import Field, Session, SQLModel, create_engine

class Swaps(SQLModel, table=True):
    
    id: Optional[int] = Field(default=None, primary_key=True)
    maker_coin: str = ""
    taker_coin: str = ""
    uuid: str = "77777777-7777-7777-7777-777777777777"
    started_at: int = 1777777777
    finished_at: int = 1777777777
    maker_amount: Decimal = 777.777777
    taker_amount: Decimal = 777.777777
    is_success: str = 1
    maker_coin_ticker: str = ""
    maker_coin_platform: str = ""
    taker_coin_ticker: str = ""
    taker_coin_platform: str = ""
    maker_coin_usd_price: Decimal = 777.777777
    taker_coin_usd_price: Decimal = 777.777777
    price: Decimal = 777.777777
    reverse_price: Decimal = 777.777777
    maker_gui: str = "unknown"
    taker_gui: str = "unknown"
    maker_version: str = "unknown"
    taker_version: str = "unknown"

class StatsSwaps(Swaps):
    __tablename__ = "stats_swaps"    

class CipiSwaps(Swaps):
    __tablename__ = "swaps"    
    