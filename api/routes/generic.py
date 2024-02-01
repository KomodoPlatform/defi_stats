#!/usr/bin/env python3
import util.cron as cron
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import Optional
import db
from lib.pair import Pair
from lib.generic import Generic
from models.generic import ErrorMessage
from util.enums import TradeType
from util.logger import logger
import util.helper as helper
import util.memcache as memcache
import util.validate as validate

generic = Generic()
router = APIRouter()

# These endpoints not yet active. Their intent is to
# expose generic cached data which is reformatted for target specific
# endpoints. For example, the 'markets' and 'gecko' endpoints for ticker
# and pairs are 90% the same, tho might use different keys or value types
# for some items.


@router.get(
    "/tickers",
    description="24-hour price & volume for each pair traded in last 90 days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def tickers():
    try:
        return memcache.get_tickers()
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/last_traded",
    description="Time and price of last trade for all pairs. Segwit pairs are merged.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def last_traded(pair_str: str = ""):
    try:
        last_traded = memcache.get_last_traded()
        if pair_str != "":
            if pair_str in last_traded:
                return last_traded[pair_str]
        return last_traded
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/orderbook/{pair_str}",
    description="Returns live orderbook for a compatible pair (e.g. `KMD_LTC` ).",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def orderbook(pair_str: str = "KMD_LTC", depth: int = 100, all: bool = True):
    try:
        return generic.orderbook(pair_str=pair_str, all=all, depth=depth, no_thread=True)
    except Exception as e:  # pragma: no cover
        err = {"error": f"{type(e)}: {e}"}
        return JSONResponse(status_code=400, content=err)


# TODO: Cache this
@router.get(
    "/historical_trades/{pair_str}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def historical_trades(
    trade_type: TradeType = TradeType.ALL,
    pair_str: str = "KMD_LTC",
    limit: int = 100,
    start_time: Optional[int] = 0,
    end_time: Optional[int] = 0,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate.positive_numeric(value, name)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        pair = Pair(pair_str=pair_str)
        data = pair.historical_trades(
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


# TODO: Cache this
@router.get(
    "/swaps_for_pair/{pair_str}",
    description="Swaps in DB for a given time range. Use format `KMD_LTC`",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def swaps_for_pair(
    trade_type: TradeType = TradeType.ALL,
    pair_str: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = 0,
    end_time: int = 0,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate.positive_numeric(value, name)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        if trade_type not in ["all", "buy", "sell"]:
            raise ValueError("trade_type must be one of: 'all', 'buy', 'sell'")
        validate.pair(pair_str)
        base, quote = helper.base_quote_from_pair(pair_str)
        days = (end_time - start_time) / 86400
        msg = f"{base}/{quote} ({trade_type}) | limit {limit} "
        msg += f"| {start_time} -> {end_time} | {days} days"
        pg_query = db.SqlQuery()
        data = pg_query.get_swaps_for_pair(base, quote, start_time, end_time, all=True)
        return {
            "trade_type": trade_type,
            "pair_str": pair_str,
            "limit": limit,
            "start_time": start_time,
            "end_time": end_time,
            "swaps_count": len(data),
            "swaps": data,
        }
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/last_24h_swaps",
    description="All successful swaps in the last 24hrs",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def last_24h_swaps():
    try:
        start_time = int(cron.now_utc()) - 86400
        end_time = int(cron.now_utc())
        query = db.SqlQuery()
        resp = query.get_swaps(start_time=start_time, end_time=end_time)
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
