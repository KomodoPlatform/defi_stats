from pydantic import BaseModel
from typing import List
from models.generic import GenericTickersInfo

# docs.google.com/document/d/1v27QFoQq1SKT3Priq3aqPgB70Xd_PnDzbOCiuoCyixw


# gecko/pairs
class GeckoPairsItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    pool_id: str = "XXX_YYY"
    base: str = "XXX"
    target: str = "YYY"
    variants: List[str] = ["XXX_YYY", "XXX-segwit_YYY", "XXX_YYY-BEP20"]


# inside gecko/tickers
class GeckoTickersItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    pool_id: str = "XXX_YYY"
    base_currency: str = "XXX"
    target_currency: str = "YYY"
    base_volume: str = "777.777777"
    target_volume: str = "777.777777"
    bid: str = "777.777777"
    ask: str = "777.777777"
    high: str = "777.777777"
    low: str = "777.777777"
    trades_24hr: str = "777"
    last_price: str = "777.777777"
    last_trade: str = "777.777777"
    last_swap_uuid: str = "77777777-7777-7777-7777-777777777777"
    volume_usd_24hr: str = "777.777777"
    liquidity_usd: str = "777.777777"
    variants: List[str] = ["XXX_YYY", "XXX-segwit_YYY", "XXX_YYY-BEP20"]


# gecko/tickers
class GeckoTickers(GenericTickersInfo):
    last_update: str = "1777777777"
    pairs_count: str = "1777777777"
    swaps_count: str = "1777777777"
    combined_volume_usd: str = "777.777777"
    combined_liquidity_usd: str = "777.777777"
    data: List[GeckoTickersItem]


# gecko/orderbook
class GeckoOrderbook(BaseModel):
    ticker_id: str = "XXX_YYY"
    timestamp: str = "1777777777"
    bids: List[List] = [["777.777777", "777.777777"]]
    asks: List[List] = [["777.777777", "777.777777"]]


# inside gecko/historical_trades
class GeckoBuyItem(BaseModel):
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "777.777777"
    base_volume: str = "777.777777"
    target_volume: str = "777.777777"
    timestamp: str = "1777777777"
    type: str = "buy"


# inside gecko/historical_trades
class GeckoSellItem(BaseModel):
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "777.777777"
    base_volume: str = "777.777777"
    target_volume: str = "777.777777"
    timestamp: str = "1777777777"
    type: str = "sell"


# gecko/historical_trades
class GeckoHistoricalTrades(BaseModel):
    ticker_id: str = "XXX_YYY"
    start_time: str = "1777777777"
    end_time: str = "1777777777"
    limit: str = "777"
    trades_count: str = "777"
    sum_base_volume_buys: str = "777.777777"
    sum_target_volume_buys: str = "777.777777"
    sum_base_volume_sells: str = "777.777777"
    sum_target_volume_sells: str = "777.777777"
    average_price: str = "777.777777"
    buy: List[GeckoBuyItem]
    sell: List[GeckoSellItem]
