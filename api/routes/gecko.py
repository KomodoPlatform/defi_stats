#!/usr/bin/env python3
from fastapi import APIRouter, Response
from fastapi_utils.tasks import repeat_every
from fastapi.responses import JSONResponse
from decimal import Decimal
from typing import List
from logger import logger
from models import (
    GeckoPairsItem,
    GeckoTickersSummary,
    GeckoOrderbookItem,
    GeckoHistoricalTradesItem,
    ErrorMessage
)
from cache import Cache
from pair import Pair
from orderbook import Orderbook
from enums import TradeType

router = APIRouter()
cache = Cache()

# Geckop Validation


def validate_ticker_id(ticker_id, valid_tickers):
    if ticker_id not in valid_tickers:
        msg = f"ticker_id '{ticker_id}' not in available pairs."
        msg += " Check the /api/v3/gecko/pairs endpoint for valid values."
        raise ValueError(msg)

def validate_positive_numeric(value, name, is_int=False):
    try:
        if Decimal(value) < 0:
            raise ValueError(f"{name} can not be negative!")
        if is_int and Decimal(value) % 1 != 0:
            raise ValueError(f"{name} must be an integer!")
    except Exception as e:
        logger.warning(f"{type(e)} Error validating {name}: {e}")
        raise ValueError(f"{name} must be numeric!")

# Gecko Caching

@router.on_event("startup")
@repeat_every(seconds=120)
def cache_gecko_data():  # pragma: no cover
    try:
        cache.save.save_gecko_source()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=90)
def cache_gecko_pairs():  # pragma: no cover
    try:
        cache.save.save_gecko_pairs()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=180)
def cache_gecko_tickers():  # pragma: no cover
    try:
        cache.save.save_gecko_tickers()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_tickers]: {e}")


# Gecko Endpoints
@router.get(
    '/pairs',
    response_model=List[GeckoPairsItem],
    description="a list pairs with price data traded within the week."
)
async def gecko_pairs():
    try:
        return cache.load.load_gecko_pairs()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}"}


@router.get(
    '/tickers',
    response_model=GeckoTickersSummary,
    description="24-hour price & volume for each market pair traded in last 7 days."
)
def gecko_tickers():
    try:
        return cache.load.load_gecko_tickers()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}


@router.get(
    '/orderbook/{ticker_id}',
    description="Provides current order book information for the given market pair.",
    response_model=GeckoOrderbookItem,
    responses={406: {"model": ErrorMessage}},
    status_code=200
)
def gecko_orderbook(response: Response, ticker_id: str = "KMD_LTC", depth: int = 100):
    try:
        gecko_pairs = cache.load.load_gecko_pairs()
        valid_tickers = [ticker['ticker_id'] for ticker in gecko_pairs]
        validate_ticker_id(ticker_id, valid_tickers)
        return Orderbook(Pair(ticker_id)).for_pair(endpoint=True, depth=depth)
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)


@router.get(
    '/historical_trades/{ticker_id}',
    description="Data for completed trades for a given market pair.",
    response_model=GeckoHistoricalTradesItem,
    responses={406: {"model": ErrorMessage}},
    status_code=200
)
def gecko_historical_trades(
        response: Response,
        trade_type: TradeType = 'all',
        ticker_id: str = "KMD_LTC",
        limit: int = 100,
        start_time: int = 0,
        end_time: int = 0
):
    try:
        gecko_pairs = cache.load.load_gecko_pairs()
        valid_tickers = [ticker['ticker_id'] for ticker in gecko_pairs]
        validate_ticker_id(ticker_id, valid_tickers)
        for value, name in [
            (limit, 'limit'),
            (start_time, 'start_time'),
            (end_time, 'end_time')
        ]:
            validate_positive_numeric(value, name)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        if trade_type not in ['all', 'buy', 'sell']:
            raise ValueError("trade_type must be one of: 'all', 'buy', 'sell'")
        pair = Pair(ticker_id)
        return pair.historical_trades(
            trade_type=trade_type,
            limit=limit,
            start_time=start_time,
            end_time=end_time
        )
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)
