#!/usr/bin/env python3
from fastapi import APIRouter, Response, status
from fastapi_utils.tasks import repeat_every
from fastapi.responses import JSONResponse

from pydantic import BaseModel
from typing import List
from logger import logger
import models
import const
import time
from helper import validate_ticker

router = APIRouter()
cache = models.Cache()

# Gecko caching functions


@router.on_event("startup")
@repeat_every(seconds=120)
def cache_gecko_data():  # pragma: no cover
    try:
        cache.save.gecko_source()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=90)
def cache_gecko_pairs():  # pragma: no cover
    try:
        cache.save.gecko_pairs()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=180)
def cache_gecko_tickers():  # pragma: no cover
    try:
        cache.save.gecko_tickers()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_ticker_data]: {e}")


# Gecko Response Models


class PairsItem(BaseModel):
    ticker_id: str = "KMD_DGB"
    pool_id: str = "KMD_DGB"
    base: str = "KMD"
    target: str = "DGB"


class TickersItem(BaseModel):
    ticker_id: str = "KMD_LTC"
    base_currency: str = "KMD"
    target_currency: str = "LTC"
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
    last_update: int = 1697383557
    pairs_count: int = 9999999999
    swaps_count: int = 9999999999
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
    ticker_id: str = "KMD_LTC"
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


class ErrorMessage(BaseModel):
    error: str = ""

    
# Gecko Format Endpoints
@router.get(
    '/pairs',
    response_model=List[PairsItem],
    description="Returns a list of all pairs with price data traded within the last 'x' days."
)
async def gecko_pairs():
    try:
        return cache.load.gecko_pairs()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}"}


@router.get(
    '/tickers',
    response_model=TickersSummary,
    description="24-hour price & volume for each market pair traded in last 7 days."
)
def gecko_tickers():
    try:
        return cache.load.gecko_tickers()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}


@router.get(
    '/orderbook/{ticker_id}',
    description="Provides current order book information for the given market pair.",
    response_model=OrderbookItem,
    responses={406: {"model": ErrorMessage}},
    status_code=200
)
def gecko_orderbook(response: Response, ticker_id: str = "KMD_LTC", depth: int = 100):
    try:
        if len(ticker_id.split("_")) != 2:
            raise ValueError(f"Invalid ticker_id: {ticker_id}")
        return models.Orderbook(models.Pair(ticker_id)).for_pair(endpoint=True, depth=depth)
    except Exception as e:  # pragma: no cover
        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        err = {"error": f"{type(e)} Error in /api/v3/gecko/orderbook [{ticker_id}] [depth: {depth}]]: {e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)


@router.get(
    '/historical_trades/{ticker_id}',
    description="Used to return data on historical completed trades for a given market pair.",
    response_model=HistoricalTradesItem
)
def gecko_historical_trades(
        trade_type: const.TradeType = 'all',
        ticker_id: str = "KMD_LTC",
        limit: int = 100,
        start_time: int = int(time.time()) - 86400,
        end_time: int = int(time.time())
):
    try:
        validate_ticker(ticker_id)
        pair = models.Pair(ticker_id)
        return pair.historical_trades(
            trade_type=trade_type,
            limit=limit,
            start_time=start_time,
            end_time=end_time
        )
    except Exception as e:  # pragma: no cover
        logger.warning(
            f"{type(e)} Error in /api/v3/gecko/historical_trades [{ticker_id}]: {e}")
        return {"error": f"{type(e)} Error in /api/v3/gecko/historical_trades [{ticker_id}]: {e}"}
