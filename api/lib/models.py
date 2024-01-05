from pydantic import BaseModel
from typing import List, Dict


# gecko/tickers,
class GenericTickersItem(BaseModel):
    ticker_id: str = "XXX_YYY"
    base_currency: str = "XXX"
    base_volume: str = "777.777777"
    target_currency: str = "YYY"
    target_volume: str = "777.777777"
    highest_bid: str = "777.777777"
    lowest_ask: str = "777.777777"
    highest_price_24hr: str = "777.777777"
    lowest_price_24hr: str = "777.777777"
    liquidity_in_usd: str = "777.777777"
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
    price_change_percent_24hr: str = "777.777777"


class ErrorMessage(BaseModel):
    error: str = ""
    message: str = ""


class ApiIds(BaseModel):
    timestamp: int = 1777777777
    ids: Dict[str, str] = {"BTC": "bitcoin", "KMD": "komodo"}


class UsdVolume(BaseModel):
    usd_volume_24hr: float = 1234567.89


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


class HealthCheck(BaseModel):
    timestamp: int = 1777777777
    status: str = "ok"


class GenericPairsInfo(BaseModel):
    # TODO: Generics endpoints and models.
    timestamp: int = 1777777777
    status: str = "ok"
