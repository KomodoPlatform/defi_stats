#!/usr/bin/env python3
from fastapi import APIRouter
from lib.cache import Cache
from lib.models import GenericTickersInfo, ErrorMessage, GenericPairsInfo
from util.enums import NetId
from util.logger import logger
from const import MARKETS_DAYS

router = APIRouter()
cache = Cache()

# These endpoints not yet active. Their intent is to
# expose generic cached data which is reformatted for target specific
# endpoints. For example, the 'markets' and 'gecko' endpoints for ticker
# and pairs are 90% the same, tho might use different keys or value types
# for some items.


@router.get(
    "/tickers",
    response_model=GenericTickersInfo,
    description=f"24-hour price & volume for each market pair traded in last {MARKETS_DAYS} days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def tickers(netid: NetId = NetId.ALL):
    try:
        cache = Cache(netid="ALL")
        return cache.get_item(name="generic_tickers").data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/generic/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/generic/tickers]: {e}"}


@router.get(
    "/pairs",
    response_model=GenericPairsInfo,
    description=f"24-hour price & volume for each market pair traded in last {MARKETS_DAYS} days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def pairs(netid: NetId = NetId.ALL):
    try:
        cache = Cache(netid="ALL")
        return cache.get_item(name="generic_pairs").data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/generic/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/generic/pairs]: {e}"}
