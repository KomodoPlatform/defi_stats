
# Gecko Response Models
from pydantic import BaseModel
from typing import List


class PairsItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    pool_id: str = "XXX_YYY"
    base: str = "XXX"
    target: str = "YYY"


class TickersItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    base_currency: str = "XXX"
    target_currency: str = "YYY"
    last_price: str = "123.456789"
    last_trade: str = "1700050000"
    trades_24hr: str = "123"
    base_volume: str = "123.456789"
    target_volume: str = "123.456789"
    bid: str = "123.456789"
    ask: str = "123.456789"
    high: str = "123.456789"
    low: str = "123.456789"
    volume_usd_24hr: str = "123.456789"
    liquidity_in_usd: str = "123.456789"


class TickersSummary(BaseModel):
    last_update: str = "1697383557"
    pairs_count: str = "9999999999"
    swaps_count: str = "9999999999"
    combined_volume_usd: str = "123.456789"
    combined_liquidity_usd: str = "123.456789"
    data: List[TickersItem]


class OrderbookItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    # base: str = "XXX"
    # quote: str = "YYY"
    timestamp: str = "1700050000"
    bids: List[List] = [["123.456789", "123.456789"]]
    asks: List[List] = [["123.456789", "123.456789"]]
    # total_asks_base_vol: str = "123.456789"
    # total_bids_base_vol: str = "123.456789"
    # total_asks_quote_vol: str = "123.456789"
    # total_bids_quote_vol: str = "123.456789"
    # total_asks_base_usd: str = "123.456789"
    # total_bids_quote_usd: str = "123.456789"
    # liquidity_usd: str = "123.456789"


class BuyItem(BaseModel):
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "123.456789"
    base_volume: str = "1"
    target_volume: str = "123.456789"
    timestamp: str = "1700050000"
    type: str = "buy"


class SellItem(BaseModel):
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "123.456789"
    base_volume: str = "123.456789"
    target_volume: str = "123.456789"
    timestamp: str = "1700050000"
    type: str = "sell"


class HistoricalTradesItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    start_time: str = "1600050000"
    end_time: str = "1700050000"
    limit: str = "100"
    trades_count: str = "5"
    sum_base_volume_buys: str = "123.456789"
    sum_target_volume_buys: str = "123.456789"
    sum_base_volume_sells: str = "123.456789"
    sum_target_volume_sells: str = "123.456789"
    average_price: str = "123.456789"
    buy: List[BuyItem]
    sell: List[SellItem]


class SwapItem(BaseModel):
    maker_coin: str = "XXX"
    taker_coin: str = "YYY"
    uuid: str = "77777777-db0e-4229-9143-f05cd0faa7e1"
    started_at: str = "1700000000"
    finished_at: str = "1700000000"
    maker_amount: str = "123.456789"
    taker_amount: str = "123.456789"
    is_success: str = 1
    maker_coin_ticker: str = "XXX"
    maker_coin_platform: str = "segwit"
    taker_coin_ticker: str = "YYY"
    taker_coin_platform: str = "BEP20"
    maker_coin_usd_price: str = "123.456789"
    taker_coin_usd_price: str = "123.456789"


# Generic Base Models

class ErrorMessage(BaseModel):
    error: str = ""
    message: str = ""
