from pydantic import BaseModel
from typing import List, Dict, Any


# gecko/tickers,
class GenericTickersItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    base_currency: str = "XXX"
    base_volume: str = "777.777777"
    quote_currency: str = "YYY"
    quote_volume: str = "777.777777"
    highest_bid: str = "777.777777"
    lowest_ask: str = "777.777777"
    highest_price_24hr: str = "777.777777"
    lowest_price_24hr: str = "777.777777"
    liquidity_usd: str = "777.777777"
    last_trade: str = "1777777777"


# wraps gecko/tickers,
class GenericTickersInfo(BaseModel):
    last_update: str = "1777777777"
    pairs_count: str = "9999999999"
    swaps_count: str = "9999999999"
    combined_volume_usd: str = "777.777777"
    combined_liquidity_usd: str = "777.777777"


class GenericOrderbookItem(BaseModel):
    pair: str = "XXX_YYY"
    base: str = "XXX"
    quote: str = "YYY"
    timestamp: str = "1777777777"
    bids: List[List] = [["777.777777", "777.777777"]]
    asks: List[List] = [["777.777777", "777.777777"]]
    total_asks_base_vol: str = "777.777777"
    total_bids_base_vol: str = "777.777777"
    total_asks_quote_vol: str = "777.777777"
    total_bids_quote_vol: str = "777.777777"
    total_asks_base_usd: str = "777.777777"
    total_bids_quote_usd: str = "777.777777"
    liquidity_usd: str = "777.777777"
    trades_24hr: str = "777"
    volume_usd_24hr: str = "777.777777"
    price_change_pct_24hr: str = "777.777777"


class GenericPairsData(BaseModel):
    ticker_id: str = "XXX_YYY"
    pool_id: str = "XXX_YYY"
    base: str = "XXX"
    quote: str = "YYY"
    priced: bool = True


class GenericPairs(BaseModel):
    timestamp: int = 1777777777
    data: List[GenericPairsData]


class GenericLastTradedData(BaseModel):
    swap_count: int = 777
    last_swap_price: float = 777.777777
    last_swap_time: int = 1777777777
    last_swap_uuid: str = "77777777-7777-7777-7777-777777777777"
    base_volume_24hr: float = 777.777777
    trade_volume_usd_24hr: float = 777.777777
    base_volume_usd_24hr: float = 777.777777
    quote_volume_24hr: float = 777.777777
    quote_volume_usd_24hr: float = 777.777777
    priced: bool = True


class GenericLastTraded(BaseModel):
    last_updated: int = 1777777777
    data: Dict[str, GenericLastTradedData]


# TODO: Move everython below into base.py
class ErrorMessage(BaseModel):
    error: str = ""
    message: str = ""


class ApiIds(BaseModel):
    timestamp: int = 1777777777
    ids: Dict[str, str] = {"BTC": "bitcoin", "KMD": "komodo"}


class UsdVolume(BaseModel):
    usd_volume_24hr: float = 7777.7777


class SwapUuids(BaseModel):
    pair: str = "KMD_LTC"
    swap_count: int = 777
    swap_uuids: List[str]


class FixerRates(BaseModel):
    timestamp: int = 1777777777
    date: str = "2777-07-27 17:17:17"
    base: str = "XXX"
    rates: Dict[str, float] = {"XXX": "777.777777", "YYY": "777.777777"}


class SwapItem(BaseModel):
    maker_coin: str = "XXX"
    taker_coin: str = "YYY"
    uuid: str = "77777777-7777-7777-7777-777777777777"
    started_at: str = "1777777777"
    finished_at: str = "1777777777"
    maker_amount: str = "777.777777"
    taker_amount: str = "777.777777"
    is_success: str = 1
    maker_coin_ticker: str = "XXX"
    maker_coin_platform: str = "segwit"
    taker_coin_ticker: str = "YYY"
    taker_coin_platform: str = "BEP20"
    maker_coin_usd_price: str = "777.777777"
    taker_coin_usd_price: str = "777.777777"
    price: float = 777.777777
    reverse_price: float = 777.777777


class HealthCheck(BaseModel):
    timestamp: int = 1777777777
    status: str = "ok"
    cache_age_mins: Dict[str, Any]


class CoinTradeVolume(BaseModel):
    swaps: int = 777
    taker_volume: float = 777.777777
    maker_volume: float = 777.777777
    trade_volume: float = 777.777777
    taker_volume_usd: float = 777.777777
    maker_volume_usd: float = 777.777777
    trade_volume_usd: float = 777.777777


class CoinTradeVolumes(BaseModel):
    swaps: int = 77777
    range_days: float = 777.77
    start_time: int = 1777777777
    end_time: int = 1777777777
    taker_volume_usd: float = 777.777777
    maker_volume_usd: float = 777.777777
    trade_volume_usd: float = 777.777777
    volumes: Dict[str, Dict[str, CoinTradeVolume]]


class PairTradeVolume(BaseModel):
    swaps: int = 777
    dex_price: float = 777.777777
    trade_volume_usd: float = 777.777777
    base_volume: float = 777.777777
    base_volume_usd: float = 777.777777
    quote_volume: float = 777.777777
    quote_volume_usd: float = 777.777777


class PairTradeVolumes(BaseModel):
    swaps: int = 77777
    range_days: float = 777.77
    start_time: int = 1777777777
    end_time: int = 1777777777
    base_volume_usd: float = 777.777777
    quote_volume_usd: float = 777.777777
    trade_volume_usd: float = 777.777777
    volumes: Dict[str, Dict[str, PairTradeVolume]]


class MonthlyPairStats(BaseModel):
    pair: str
    swap_count: int
    volume: float


class MonthlyPubkeyStats(BaseModel):
    pubkey: str
    swap_count: int
    volume: float


class MonthlyGuiStats(BaseModel):
    gui: str
    swap_count: int
    pubkey_count: int


class MonthlyStatsItem(BaseModel):
    month: int  # 1-12
    total_swaps: int
    top_pairs: list[MonthlyPairStats]
    unique_pubkeys: int
    top_pubkeys: list[MonthlyPubkeyStats]
    gui_stats: list[MonthlyGuiStats]
    pubkey_gui_counts: dict  # {1: int, 2: int, 3: int, '3+': int}


class MonthlyStatsResponse(BaseModel):
    year: int
    months: list[MonthlyStatsItem]
