#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import List, Dict
from lib.cache import Cache
from lib.cache_calc import CMC
from lib.pair import Pair
from models.generic import ErrorMessage
from models.cmc import CmcAsset, CmcTrades, CmcSummary, CmcOrderbook, CmcTicker
from util.cron import cron
from util.logger import logger
from util.transform import invert, sortdata, convert
import util.memcache as memcache
import util.validate as validate

router = APIRouter()
cache = Cache()



@router.get(
    "/assets",
    description="A detailed summary for each currency available on the exchange.",
    responses={406: {"model": ErrorMessage}},
    # response_model=List[CmcSummary],
    status_code=200,
)
def summary():
    try:
        return CMC().assets()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats-api/assets]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats-api/assets]: {e}"}


# TODO: Cache this
@router.get(
    "/summary",
    description="Pair summary for last 24 hours for all pairs traded in last 7 days.",
    responses={406: {"model": ErrorMessage}},
    response_model=List[CmcSummary],
    status_code=200,
)
def summary():
    try:
        return CMC().summary()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats-api/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats-api/summary]: {e}"}


@router.get(
    "/ticker",
    description="Simple last price and liquidity for each market pair, traded in last 7 days.",
    response_model=List[Dict[str, CmcTicker]],
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def ticker():
    try:
        return CMC().tickers()
        # resp.append(convert.book_to_stats_api_ticker())
        # return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats-api/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats-api/ticker]: {e}"}


@router.get(
    "/orderbook/{pair_str}",
    description="Returns live orderbook for a compatible pair (e.g. `KMD_LTC` ).",
    response_model=CmcOrderbook,
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
        logger.info(resp)
        resp["timestamp"] = int(resp["timestamp"]) * 1000
        return resp

    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/trades/{pair_str}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    response_model=List[CmcTrades],
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
        resp = [convert.historical_trades_to_cmc(i) for i in resp]
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
