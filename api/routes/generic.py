#!/usr/bin/env python3
import time
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from lib.cache import Cache
from lib.generic import Generic
from lib.pair import Pair
from util.enums import TradeType
from util.validate import validate_positive_numeric
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
    description="Time and price of last trade for all pairs",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def last_traded():
    try:
        return load_generic_last_traded()
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/orderbook/{ticker_id}",
    description="Returns live orderbook for a compatible pair (e.g. `KMD_LTC` ).",
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
    "/historical_trades/{ticker_id}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def historical_trades(
    trade_type: TradeType = TradeType.ALL,
    ticker_id: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = int(time.time() - 86400),
    end_time: int = int(time.time()),
):
    try:
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate_positive_numeric(value, name)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        if trade_type not in ["all", "buy", "sell"]:
            raise ValueError("trade_type must be one of: 'all', 'buy', 'sell'")
        pair = Pair(pair_str=ticker_id)
        data = pair.historical_trades(
            trade_type=trade_type,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
