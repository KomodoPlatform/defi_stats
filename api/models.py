# Gecko Response Models
from pydantic import BaseModel
from typing import List, Dict


# Generic Base Models


class GenericTickersItem(BaseModel):
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


class GenericTickersInfo(BaseModel):
    last_update: str = "1697383557"
    pairs_count: str = "9999999999"
    swaps_count: str = "9999999999"
    combined_volume_usd: str = "123.456789"
    combined_liquidity_usd: str = "123.456789"
    data: Dict[str, GenericTickersItem]


# Generic Base Models


class GeckoPairsItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    pool_id: str = "XXX_YYY"
    base: str = "XXX"
    target: str = "YYY"


class GeckoTickersItem(GenericTickersItem):
    ticker_id: str = "XXX_YYY"


class GeckoTickersSummary(GenericTickersInfo):
    data: List[GeckoTickersItem]


class GeckoOrderbookItem(BaseModel):
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


class GeckoBuyItem(BaseModel):
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "123.456789"
    base_volume: str = "1"
    target_volume: str = "123.456789"
    timestamp: str = "1700050000"
    type: str = "buy"


class GeckoSellItem(BaseModel):
    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: str = "123.456789"
    base_volume: str = "123.456789"
    target_volume: str = "123.456789"
    timestamp: str = "1700050000"
    type: str = "sell"


class GeckoHistoricalTradesItem(BaseModel):
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
    buy: List[GeckoBuyItem]
    sell: List[GeckoSellItem]


class GeckoSwapItem(BaseModel):
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


# Rates Models


class FixerRates(BaseModel):
    timestamp: int = 1700050000
    date: str = "2023-10-30 16:03:03"
    base: str = "XXX"
    rates: Dict[str, float] = {"XXX": "123.456789", "YYY": "123.456789"}


# Coins Models


class ApiIds(BaseModel):
    timestamp: int = 1700050000
    ids: Dict[str, str] = {"BTC": "bitcoin", "KMD": "komodo"}


class UsdVolume(BaseModel):
    usd_volume_24h: float = 1234567.89


class CurrentLiquidity(BaseModel):
    current_liquidity: float = 1234567.89


class SwapUuids(BaseModel):
    pair: str = "KMD_LTC"
    swap_uuids: List[str]
