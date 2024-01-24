#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
import util.cron as cron
from lib.generic import Generic
from typing import List
from db.sqlitedb import get_sqlite_db
from lib.cache import Cache
from lib.pair import Pair
from models.generic import ErrorMessage
from models.stats_api import (
    StatsApiAtomicdexIo,
    StatsApiSummary,
    StatsApiOrderbook,
    StatsApiTradeInfo,
)
from util.logger import logger
import util.transform as transform
import util.validate as validate
from util.helper import get_last_trade_item
import lib

router = APIRouter()
cache = Cache()


# TODO: Move to new DB
@router.get(
    "/atomicdexio",
    description="Simple summary statistics for the Komodo Defi network.",
    response_model=StatsApiAtomicdexIo,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def atomicdexio():
    try:
        cache = Cache(netid="ALL")
        tickers_data = cache.get_item(name="generic_tickers").data
        db = get_sqlite_db(netid="ALL")
        counts = db.query.swap_counts()
        counts.update({"current_liquidity": tickers_data["combined_liquidity_usd"]})
        return counts

    except Exception as e:  # pragma: no cover
        err = {"error": f"Error in [/api/v3/stats-api/atomicdexio]: {e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


# TODO: Cache this
@router.get("/atomicdex_fortnight")
def atomicdex_fortnight():
    """Extra Summary Statistics over last 2 weeks"""
    try:
        return lib.load_adex_fortnite()
    except Exception as e:  # pragma: no cover
        msg = f"{type(e)} Error in [/api/v3/stats-api/atomicdex_fortnight]: {e}"
        logger.warning(msg)
        return {"error": msg}


# TODO: Cache this
@router.get(
    "/summary",
    description="Pair summary for last 24 hours for all pairs traded in last 7 days.",
    responses={406: {"model": ErrorMessage}},
    response_model=List[StatsApiSummary],
    status_code=200,
)
def summary():
    try:
        return lib.load_statsapi_summary()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats-api/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats-api/atomicdexio]: {e}"}


@router.get(
    "/ticker",
    description="Simple last price and liquidity for each market pair, traded in last 7 days.",
)
def ticker():
    try:
        cache = lib.Cache(netid="ALL")
        data = cache.get_item(name="markets_tickers").data
        resp = []
        for i in data["data"]:
            resp.append(transform.ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats-api/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats-api/ticker]: {e}"}


@router.get(
    "/orderbook/{ticker_id}",
    description="Returns live orderbook for a compatible pair (e.g. `KMD_LTC` ).",
    response_model=StatsApiOrderbook,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def orderbook(
    ticker_id: str = "KMD_LTC",
    depth: int = 100,
):
    try:
        generic = Generic(netid="ALL")
        data = generic.orderbook(pair_str=ticker_id, depth=depth)
        data = transform.orderbook_to_gecko(data)
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/trades/{ticker_id}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    response_model=List[StatsApiTradeInfo],
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def trades(
    ticker_id: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = int(cron.now_utc() - 86400),
    end_time: int = int(cron.now_utc()),
):
    try:
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate.positive_numeric(value, name)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        pair = Pair(pair_str=ticker_id)
        data = pair.historical_trades(
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )['ALL']
        resp = data["buy"] + data["sell"]
        resp = transform.sort_dict_list(resp, "timestamp", True)
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/last_price/{pair}",
    description="Price of last trade for pair. Use format `KMD_LTC`",
    response_model=float,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def last_price_for_pair(pair="KMD_LTC"):
    """Last trade price for a given pair."""
    try:
        last_traded_cache = lib.load_generic_last_traded()
        return get_last_trade_item(pair, last_traded_cache, "last_price")
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/last_price/{pair}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}
