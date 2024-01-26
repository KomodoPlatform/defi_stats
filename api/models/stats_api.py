from pydantic import BaseModel
from typing import Dict, List


# stats-api/atomicdexio
class StatsApiAtomicdexIo(BaseModel):
    swaps_24hr: int = 777
    swaps_30d: int = 777777
    swaps_all_time: int = 777777777777
    current_liquidity: float = 7777777.777


# stats-api/atomicdex_fortnight
class TopPairs(BaseModel):
    by_value_traded_usd: dict = Dict[str, float]
    by_current_liquidity_usd: dict = Dict[str, float]
    by_swaps_count: dict = Dict[str, int]


class StatsApiAtomicdexFortnight(BaseModel):
    days: int = 777
    swaps_count: int = 777777
    swaps_value: int = 777777777777
    current_liquidity: float = 7777777.777
    top_pairs: dict = TopPairs


class StatsApiSummary(BaseModel):
    trading_pair: str = "XXX_YYY"
    pair_swaps_count: int = 7777
    base_currency: str = "XXX"
    base_volume: float = 777.777777
    base_price_usd: float = 777.777777
    base_liquidity_coins: float = 777.777777
    base_liquidity_usd: float = 777.777777
    base_trade_value_usd: float = 777.777777
    quote_currency: str = "YYY"
    quote_volume: float = 777.777777
    quote_price_usd: float = 777.777777
    quote_liquidity_coins: float = 777.777777
    quote_liquidity_usd: float = 777.777777
    quote_trade_value_usd: float = 777.777777
    pair_liquidity_usd: float = 777.777777
    pair_trade_value_usd: float = 777.777777
    lowest_ask: float = 777.777777
    highest_bid: float = 777.777777
    highest_price_24h: float = 777.777777
    lowest_price_24h: float = 777.777777
    price_change_24h: float = 777.777777
    price_change_pct_24h: float = 777.777777
    last_price: float = 777.777777
    last_trade: int = 1777777777


class StatsApiOrderbook(BaseModel):
    pair: str = "XXX_YYY"
    timestamp: str = "1777777777"
    bids: List[List] = [["777.777777", "777.777777"]]
    asks: List[List] = [["777.777777", "777.777777"]]
    total_asks_base_vol: str = "777.777777"
    total_bids_quote_vol: str = "777.777777"


class StatsApiTradeInfo(BaseModel):
    pair: str = "XXX_YYY"
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: float = 777.777777
    base_volume: float = 777.777777
    target_volume: float = 777.777777
    timestamp: int = 1777777777
    type: str = "buy | sell"
