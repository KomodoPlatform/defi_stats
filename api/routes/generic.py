#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from lib.cache import Cache
from lib.generic import Generic
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
