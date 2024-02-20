from pydantic import BaseModel
from typing import List


# stats_xyz/atomicdexio
class StatsXyzAtomicdexIo(BaseModel):
    swaps_24hr: int = 777
    swaps_7d: int = 7777
    swaps_14d: int = 77777
    swaps_30d: int = 777777
    swaps_all_time: int = 777777777777


# stats_xyz/current_liquidity
class StatsXyzLiquidity(BaseModel):
    trades_24h: int = 777
    volume_24h: float = 777.7777


# stats_xyz/fiat_rates
# TODO: add model


# inside stats_xyz/orderbook
class StatsXyzOrderbookBidAsk(BaseModel):
    price: str = "777.777777"
    volume: str = "777.777777"
    quote_volume: str = "777.777777"


# stats_xyz/orderbook
class StatsXyzOrderbookItem(BaseModel):
    pair: str = "XXX_YYY"
    base: str = "XXX"
    quote: str = "YYY"
    trades_24hr: int = 777
    base_liquidity_coins: float = 777.777777
    base_liquidity_usd: float = 777.777777
    quote_liquidity_coins: float = 777.777777
    quote_liquidity_usd: float = 777.777777
    liquidity_usd: str = "777.777777"
    lowest_ask: float = 777.777777
    highest_bid: float = 777.777777
    highest_price_24hr: float = 777.777777
    lowest_price_24hr: float = 777.777777
    total_asks_base_vol: str = "777.777777"
    total_asks_quote_vol: str = "777.777777"
    total_bids_base_vol: str = "777.777777"
    total_bids_quote_vol: str = "777.777777"
    total_asks_base_usd: str = "777.777777"
    total_bids_quote_usd: str = "777.777777"
    volume_usd_24hr: str = "777.777777"
    price_change_24hr: float = 777.777777
    price_change_pct_24hr: float = 777.777777
    base_price_usd: float = 777.777777
    quote_price_usd: float = 777.777777
    oldest_price_time: int = 777
    oldest_price: float = 777.777777
    newest_price_time: int = 777
    newest_price: float = 777.777777
    timestamp: str = 1777777777
    bids: List[StatsXyzOrderbookBidAsk]
    asks: List[StatsXyzOrderbookBidAsk]


# stats_xyz/summary
class StatsXyzSummary(BaseModel):
    trading_pair: str = "XXX_YYY"
    trades_24h: int = 7777
    base_currency: str = "XXX"
    quote_currency: str = "YYY"
    base_volume: float = 777.777777
    quote_volume: float = 777.777777
    lowest_ask: float = 777.777777
    highest_bid: float = 777.777777
    lowest_price_24h: float = 777.777777
    highest_price_24h: float = 777.777777
    price_change_percent_24h: float = 777.777777
    last_price: float = 777.777777
    last_swap_timestamp: int = 1777777777


# stats_xyz/swaps24
class StatsXyzSwaps24(BaseModel):
    ticker: str = "XXX"
    volume: float = 777.7777
    volume_usd: float = 777.7777
    swaps_amount_24h: int = 777


# stats_xyz/ticker
class StatsXyzTickerItem(BaseModel):
    last_price: str = "777.777777"
    base_volume: str = "777.777777"
    quote_volume: str = "777.777777"
    isFrozen: str = "0"


# stats_xyz/ticker_summary
class StatsXyzTickerSummary(BaseModel):
    trades_24h: int = 777
    volume_24h: float = 777.7777


# stats_xyz/trades
class StatsXyzTrades(BaseModel):
    pair: str = "XXX_YYY"
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "123.456"
    base_volume: str = "777.777777"
    quote_volume: str = "777.777777"
    timestamp: int = 1777777777
    type: str = "sell"


# stats_xyz/usd_volume
class StatsXyzUsdVolume(BaseModel):
    usd_volume_24h: float = 777.7777
