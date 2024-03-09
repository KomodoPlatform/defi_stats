#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import List
from lib.cache import Cache
from lib.cache_calc import CacheCalc
from lib.pair import Pair
from models.generic import ErrorMessage
from models.stats_api import (
    StatsApiAtomicdexIo,
    StatsApiSummary,
    StatsApiOrderbook,
    StatsApiTradeInfo,
)
from util.cron import cron
from util.logger import logger
from util.transform import derive, invert, sortdata, convert
import db.sqldb as db
import util.memcache as memcache
import util.validate as validate

router = APIRouter()
cache = Cache()
































@router.get(
    "/atomicdexio",
    description="Simple summary statistics for the Komodo Defi network.",
    response_model=StatsApiAtomicdexIo,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def atomicdexio():
    try:
        query = db.SqlQuery()
        data = query.swap_counts()
        extras = memcache.get_adex_24hr()
        data.update(
            {
                "current_liquidity": extras["current_liquidity"],
                "volume_24hr": extras["swaps_volume"],
            }
        )
        return data

    except Exception as e:  # pragma: no cover
        err = {"error": f"Error in [/api/v3/stats-api/atomicdexio]: {e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


# TODO: Cache this
@router.get("/atomicdex_24hr")
def atomicdex_24hr():
    """Extra Summary Statistics over last 24 hrs"""
    try:
        return CacheCalc().adex_24hr()
    except Exception as e:  # pragma: no cover
        msg = f"{type(e)} Error in [/api/v3/stats-api/atomicdex_24hr]: {e}"
        logger.warning(msg)
        return {"error": msg}


@router.get("/atomicdex_fortnight")
def atomicdex_fortnight():
    """Extra Summary Statistics over last 2 weeks"""
    try:
        return CacheCalc().adex_fortnite()
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
        data = CacheCalc().stats_api_summary()
        return data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats-api/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats-api/atomicdexio]: {e}"}


@router.get(
    "/ticker",
    description="Simple last price and liquidity for each market pair, traded in last 7 days.",
)
def ticker():
    try:
        c = CacheCalc()
        return c.tickers_lite(depaired=True)
        # resp.append(convert.book_to_stats_api_ticker())
        # return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats-api/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats-api/ticker]: {e}"}


@router.get(
    "/orderbook/{pair_str}",
    description="Returns live orderbook for a compatible pair (e.g. `KMD_LTC` ).",
    response_model=StatsApiOrderbook,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def orderbook(
    pair_str: str = "KMD_LTC",
    depth: int = 100,
):
    try:
        gecko_source = memcache.get_gecko_source()
        is_reversed = pair_str != sortdata.pair_by_market_cap(
            pair_str, gecko_source=gecko_source
        )
        if is_reversed:
            pair = Pair(pair_str=invert.pair(pair_str), gecko_source=gecko_source)
            data = pair.orderbook(pair_str=invert.pair(pair_str), depth=depth)
        else:
            pair = Pair(pair_str=pair_str, gecko_source=gecko_source)
            data = pair.orderbook(pair_str=pair_str, depth=depth)

        resp = data["ALL"]
        if is_reversed:
            resp = invert.pair_orderbook(resp)
        resp.update(
            {
                "variants": sorted(list(set(data.keys()))),
                "asks": [
                    [convert.format_10f(i["price"]), convert.format_10f(i["volume"])]
                    for i in resp["asks"]
                ][:depth],
                "bids": [
                    [convert.format_10f(i["price"]), convert.format_10f(i["volume"])]
                    for i in resp["bids"]
                ][:depth],
                "total_asks_base_vol": resp["base_liquidity_coins"],
                "total_bids_quote_vol": resp["quote_liquidity_coins"],
            }
        )
        return resp

    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/trades/{pair_str}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    response_model=List[StatsApiTradeInfo],
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def trades(
    pair_str: str = "KMD_LTC", limit: int = 100, start_time: int = 0, end_time: int = 0
):
    try:
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate.positive_numeric(value, name)
        if start_time == 0:
            start_time = int(cron.now_utc() - 86400)
        if end_time == 0:
            end_time = int(cron.now_utc())
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        pair = Pair(pair_str=pair_str)
        data = pair.historical_trades(
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )["ALL"]
        resp = data["buy"] + data["sell"]
        resp = sortdata.dict_lists(resp, "timestamp", True)
        resp = [convert.historical_trades_to_stats_api(i) for i in resp]
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/last_price/{pair_str}",
    description="Price of last trade for pair. Use format `KMD_LTC`",
    response_model=float,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def last_price_for_pair(pair_str="KMD_LTC"):
    """Last trade price for a given pair."""
    try:
        pairs_last_traded_cache = memcache.get_pairs_last_traded()
        data = derive.last_trade_info(
            pair_str, pairs_last_traded_cache=pairs_last_traded_cache
        )
        return data["ALL"]["last_swap_price"]
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/last_price/{pair_str}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}
