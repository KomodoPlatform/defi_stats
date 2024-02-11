from pydantic import BaseModel
from typing import List
from models.generic import GenericTickersInfo, GenericTickersItem

# docs.google.com/document/d/1v27QFoQq1SKT3Priq3aqPgB70Xd_PnDzbOCiuoCyixw


# markets/atomicdexio
class MarketsAtomicdexIo(BaseModel):
    swaps_24hr: int = 999999999
    swaps_7d: int = 999999999
    swaps_14d: int = 999999999
    swaps_30d: int = 999999999
    swaps_all_time: int = 999999999


# markets/current_liquidity
class MarketsCurrentLiquidity(BaseModel):
    current_liquidity: float = 7777.7777


# markets/fiat_rates
class MarketsFiatRatesItem(BaseModel):
    usd_market_cap: float = 7777.7777
    usd_price: float = 7777.7777
    coingecko_id: str = "coin_id"


class MarketsOrderbookBid(BaseModel):
    price: str = "777.777777"
    volume: str = "777.777777"


class MarketsOrderbookAsk(MarketsOrderbookBid):
    pass


# markets/orderbook
class MarketsOrderbookItem(BaseModel):
    pair: str = "XXX_YYY"
    timestamp: str = 1777777777
    base: str = "XXX"
    quote: str = "YYY"
    trades_24hr: int = 777
    liquidity_usd: str = "777.777777"
    volume_usd_24hr: str = "777.777777"
    # total_asks_base_vol: str = "777.777777"
    # total_asks_quote_vol: str = "777.777777"
    # total_asks_base_usd: str = "777.777777"
    # total_bids_base_vol: str = "777.777777"
    # total_bids_quote_vol: str = "777.777777"
    # total_bids_quote_usd: str = "777.777777"
    bids: List[MarketsOrderbookBid]
    asks: List[MarketsOrderbookAsk]


# markets/pairs_last_trade
class MarketsPairLastTradeItem(BaseModel):
    pair: str = "XXX_YYY"
    swap_count: int = 777
    last_swap: int = 1777777777
    last_swap_uuid: str = "77777777-7777-7777-7777-777777777777"
    last_price: float = 777.777777
    last_taker_amount: float = 777.777777
    last_maker_amount: float = 777.777777
    first_price: float = 777.777777
    first_taker_amount: float = 777.777777
    first_maker_amount: float = 777.777777
    sum_taker_traded: float = 777.777777
    sum_maker_traded: float = 777.777777


# markets/summary
class MarketsSummaryItem(BaseModel):
    pair: str = "XXX_YYY"
    base_currency: str = "XXX"
    quote_currency: str = "YYY"
    trades_24hr: int = 777
    variants: List[str] = [
        "XXX_YYY",
        "XXX-segwit_YYY",
        "XXX_YYY-PLG20",
        "XXX-segwit_YYY-PLG20",
    ]
    base_volume: str = "777.777777"
    quote_volume: str = "777.777777"
    lowest_ask: str = "777.777777"
    highest_bid: str = "777.777777"
    lowest_price_24hr: str = "777.777777"
    highest_price_24hr: str = "777.777777"
    price_change_pct_24hr: str = "777.777777"
    last_price: str = "777.777777"
    last_swap_uuid: str = "777.777777"
    last_swap: int = 1777777777


# markets/usd_volume
class MarketsUsdVolume(BaseModel):
    usd_volume_24hr: float = 777.7777


# markets/swaps24
class MarketsSwaps24(BaseModel):
    ticker: str = "XXX"
    volume: int = 777
    volume_usd: float = 777.7777
    swaps_amount_24hr: float = 777.7777


# markets/summary_for_ticker
class MarketsSummaryForTickerItem(BaseModel):
    pair: str = "XXX_YYY"
    trades_24hr: int = 777
    base: str = "XXX"
    base_volume: str = "777.777777"
    base_price_usd: str = "777.777777"
    quote: str = "YYY"
    quote_volume: str = "777.777777"
    quote_price_usd: str = "777.777777"
    highest_bid: str = "777.777777"
    lowest_ask: str = "777.777777"
    liquidity_usd: str = "777.777777"
    volume_usd_24hr: str = "777.777777"
    highest_price_24hr: str = "777.777777"
    lowest_price_24hr: str = "777.777777"
    price_change_24hr: str = "777.777777"
    price_change_pct_24hr: str = "777.777777"
    last_price: str = "777.777777"
    last_trade: int = 1777777777
    last_swap_uuid: str = "77777777-7777-7777-7777-777777777777"


# markets/summary_for_ticker
class MarketsSummaryForTicker(BaseModel):
    last_update: int = 1777777777
    pairs_count: int = 777
    swaps_count: int = 777
    volume_usd_24hr: float = 777.7777
    liquidity_usd: float = 777.7777
    data: List[MarketsSummaryForTickerItem]


class PairTrades(BaseModel):
    pair: str = "XXX_YYY"
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "123.456"
    base_volume: str = "777.777777"
    quote_volume: str = "777.777777"
    timestamp: int = 1777777777
    type: str = "sell"


# markets/ticker
class MarketsTickerItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    pool_id: str = "XXX_YYY"
    base: str = "XXX"
    target: str = "YYY"


# markets/ticker
class MarketsTickerItemDetail(BaseModel):
    ticker_id: str = "XXX_YYY"
    pool_id: str = "XXX_YYY"
    base: str = "XXX"
    target: str = "YYY"


# gecko/tickers
class GeckoTickersItem(GenericTickersItem):
    ticker_id: str = "XXX_YYY"
    target_currency: str = "YYY"
    target_volume: str = "777.777777"
    bid: str = "777.777777"
    ask: str = "777.777777"
    high: str = "777.777777"
    low: str = "777.777777"
    last_trade: int = 1777777777
    # trades_24hr: int = "123"
    # volume_usd_24hr: str = "777.777777"
    # price_change_pct_24hr: str = "777.777777"


# wraps gecko/tickers
class GeckoTickersSummary(GenericTickersInfo):
    data: List[GeckoTickersItem]


class GeckoBuyItem(BaseModel):
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "777.777777"
    base_volume: str = "777.777777"
    target_volume: str = "777.777777"
    timestamp: int = 1777777777
    type: str = "buy"


class GeckoSellItem(BaseModel):
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "777.777777"
    base_volume: str = "777.777777"
    target_volume: str = "777.777777"
    timestamp: int = 1777777777
    type: str = "sell"


class GeckoHistoricalTradesItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    start_time: int = 1777777777
    end_time: int = 1777777777
    limit: int = 777
    trades_count: int = 777
    sum_base_volume_buys: str = "777.777777"
    sum_target_volume_buys: str = "777.777777"
    sum_base_volume_sells: str = "777.777777"
    sum_target_volume_sells: str = "777.777777"
    average_price: str = "777.777777"
    buy: List[GeckoBuyItem]
    sell: List[GeckoSellItem]


class GeckoSwapItem(BaseModel):
    maker_coin: str = "XXX"
    taker_coin: str = "YYY"
    uuid: str = "77777777-7777-7777-7777-777777777777"
    started_at: int = 1777777777
    finished_at: int = 1777777777
    maker_amount: str = "777.777777"
    taker_amount: str = "777.777777"
    is_success: str = 1
    maker_coin_ticker: str = "XXX"
    maker_coin_platform: str = "segwit"
    taker_coin_ticker: str = "YYY"
    taker_coin_platform: str = "BEP20"
    maker_coin_usd_price: str = "777.777777"
    taker_coin_usd_price: str = "777.777777"
