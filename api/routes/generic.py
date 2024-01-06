#!/usr/bin/env python3
from fastapi import APIRouter
from lib.cache import Cache
from models.generic import (
    GenericTickersInfo,
    ErrorMessage,
    GenericPairs,
    GenericLastTraded,
)
from util.logger import logger
from const import GENERIC_PAIRS_DAYS
from lib.cache_load import load_generic_pairs

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
    description=f"24-hour price & volume for each pair traded in last {GENERIC_PAIRS_DAYS} days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def tickers():
    try:
        cache = Cache(netid="ALL")
        return cache.get_item(name="generic_tickers").data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/generic/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/generic/tickers]: {e}"}


@router.get(
    "/pairs",
    response_model=GenericPairs,
    description=f"Pairs traded in last {GENERIC_PAIRS_DAYS} days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def pairs():
    try:
        return load_generic_pairs()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/generic/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/generic/pairs]: {e}"}


@router.get(
    "/last_traded",
    response_model=GenericLastTraded,
    description=f"Pairs traded in last {GENERIC_PAIRS_DAYS} days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def last_traded():
    try:
        return load_generic_pairs()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/generic/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/generic/pairs]: {e}"}
