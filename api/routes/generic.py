#!/usr/bin/env python3
import util.cron as cron
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import Optional
from lib.cache import Cache
from lib.generic import Generic
from lib.pair import Pair
from util.enums import TradeType
import util.validate as validate
import db
import lib
from models.generic import (
    ErrorMessage,
)
from util.logger import logger
import util.transform as transform
from const import GENERIC_PAIRS_DAYS
from lib.cache import load_generic_pairs, load_generic_last_traded, load_generic_tickers

router = APIRouter()
cache = Cache()

# These endpoints not yet active. Their intent is to
# expose generic cached data which is reformatted for target specific
# endpoints. For example, the 'markets' and 'gecko' endpoints for ticker
# and pairs are 90% the same, tho might use different keys or value types
# for some items.


@router.get(
    "/tickers",
    description=f"24-hour price & volume for each pair traded in last {GENERIC_PAIRS_DAYS} days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def tickers():
    try:
        return load_generic_tickers()
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/pairs",
    description=f"Pairs traded in last {GENERIC_PAIRS_DAYS} days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def pairs():
    try:
        return load_generic_pairs()
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
        last_traded = load_generic_last_traded()
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
def orderbook(
    pair_str: str = "KMD_LTC",
    depth: int = 100,
):
    try:
        generic = Generic(netid="ALL")
        data = generic.orderbook(pair_str=pair_str, depth=depth, all=True)
        data = transform.orderbook_to_gecko(data)
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


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
        if "_" not in pair_str:
            raise ValueError("pair_str must be in the format: 'KMD_LTC'")
        base, quote = transform.base_quote_from_pair(pair_str)
        days = (end_time - start_time) / 86400
        msg = f"{base}/{quote} ({trade_type}) | limit {limit} "
        msg += f"| {start_time} -> {end_time} | {days} days"

        coins_config = lib.load_coins_config()
        gecko_source = lib.load_gecko_source()
        pg_query = db.SqlQuery(gecko_source=gecko_source, coins_config=coins_config)
        data = pg_query.get_swaps_for_pair(
            base, quote, trade_type, limit, start_time, end_time
        )
        """
        pair = Pair(pair_str=pair_str)
        data = pair.historical_trades(
            trade_type=trade_type,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )
        """
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
        resp = query.get_swaps(
            start_time=start_time,
            end_time=end_time
        )
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
