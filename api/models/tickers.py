from pydantic import BaseModel
from typing import Dict


# gecko/tickers,
class TickersSummaryItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    trades_24hr: int = 777
    base_currency: str = "XXX"
    target_currency: str = "YYY"
    base_volume: str = "777.777777"
    target_volume: str = "777.777777"
    bid: str = "777.777777"
    ask: str = "777.777777"
    high: str = "777.777777"
    low: str = "777.777777"
    last_trade: str = "1777777777"
    last_price: str = "1777777777"
    volume_usd_24hr: str = "777.777777"
    liquidity_in_usd: str = "777.777777"


# wraps gecko/tickers,
class TickersSummary(BaseModel):
    last_update: str = "1777777777"
    pairs_count: str = "9999999999"
    swaps_count: str = "9999999999"
    combined_volume_usd: str = "777.777777"
    combined_liquidity_usd: str = "777.777777"
    data: Dict[str, TickersSummaryItem]
