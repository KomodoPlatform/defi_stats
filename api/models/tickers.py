from pydantic import BaseModel
from typing import Dict, List


# /api/v3/tickers/summary
class TickersSummaryItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    base_currency: str = "XXX"
    target_currency: str = "YYY"
    variants: List[str]
    base_volume: str = "777.777777"
    target_volume: str = "777.777777"
    bid: str = "777.777777"
    ask: str = "777.777777"
    high: str = "777.777777"
    low: str = "777.777777"
    trades_24hr: int = 777
    volume_usd_24hr: str = "777.777777"
    liquidity_in_usd: str = "777.777777"
    last_trade: int = 1777777777
    last_price: str = "1777777777"
    last_swap_uuid: str = "77777777-7777-7777-7777-777777777777"


# /api/v3/tickers/summary
class TickersSummary(BaseModel):
    last_update: int = 1777777777
    pairs_count: int = 77777
    swaps_count: int = 777
    combined_volume_usd: str = "777.777777"
    combined_liquidity_usd: str = "777.777777"
    data: Dict[str, TickersSummaryItem]
