from pydantic import BaseModel
from typing import List

# https://docs.google.com/document/d/1S4urpzUnO2t7DmS_1dc4EL4tgnnbTObPYXvDeBnukCg/edit


# cmc/summary
class CmcSummary(BaseModel):
    """
    The summary endpoint is to provide an overview of market
    data for all tickers and all market pairs on the exchange.
    """

    trading_pair: str = "XXX_YYY"
    base_currency: str = "XXX"
    quote_currency: str = "YYY"
    last_price: float = 777.777777
    lowest_ask: float = 777.777777
    highest_bid: float = 777.777777
    base_volume: float = 777.777777
    quote_volume: float = 777.777777
    price_change_percent_24h: float = 777.777777
    highest_price_24h: float = 777.777777
    lowest_price_24h: float = 777.777777


# cmc/assets
class CmcAsset(BaseModel):
    """
    The assets endpoint is to provide a detailed summary for
    each currency available on the exchange.
    """

    name: str = "XXX"
    unified_cryptoasset_id: str = "XXX"
    can_withdraw: bool = True
    can_deposit: bool = True
    min_withdraw: float = "0"
    max_withdraw: float = "21000000"
    maker_fee: str = "0"
    # Fees are not static, so we cant display universal values
    # taker_fee: str = "XXX"
    contractAddressUrl: str | None = None
    contractAddress: str | None = None


# cmc/tickers
class CmcTicker(BaseModel):
    """
    The ticker endpoint is to provide a 24-hour pricing and volume
    summary for each market pair available on the exchange.
    """

    base_id: str = ""
    quote_id: str = ""
    last_price: str = 7777777.777
    base_volume: str = 7777777.777
    quote_volume: str = 7777777.777
    isFrozen: str = "0"


# cmc/orderbook/{pair}
class CmcOrderbook(BaseModel):
    """
    The order book endpoint is to provide a complete level 2 order
    book (arranged by best asks/bids) with full depth returned for
    a given market pair.
    """

    timestamp: int = 1777777777000  # In ms
    bids: List[List] = [["777.777777", "777.777777"]]
    asks: List[List] = [["777.777777", "777.777777"]]


# cmc/trades/{pair}
class CmcTrades(BaseModel):
    """
    The trades endpoint is to return data on all
    recently completed trades for a given market pair.
    """

    trade_id: str = "77777777-7777-7777-7777-777777777777"
    price: float = 777.777777
    base_volume: float = 777.777777
    quote_volume: float = 777.777777
    timestamp: int = 1777777777000  # In ms
    type: str = "buy | sell"
