#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from db.sqlitedb import get_sqlite_db
from lib.cache import Cache
from models.generic import ErrorMessage, SwapItem
from util.exceptions import UuidNotFoundException
from util.logger import logger
import util.transform as transform


router = APIRouter()
cache = Cache()

# These endpoints not yet active. Their intent is to
# expose generic cached data which is reformatted for target specific
# endpoints. For example, the 'markets' and 'gecko' endpoints for ticker
# and pairs are 90% the same, tho might use different keys or value types
# for some items.


@router.get(
    "/get_swaps_for_pair",
    description="Swaps for a pairs traded in last 24hrs. Segwit merged & ordered by mcap.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def get_swaps_for_pair(pair_str):
    try:
        base, quote = transform.base_quote_from_pair(pair_str)
        db = get_sqlite_db(netid="ALL")
        return db.query.get_swaps_for_pair_old(base, quote)
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/get_swap/{uuid}",
    description="Get swap info from a uuid, e.g. `82df2fc6-df0f-439a-a4d3-efb42a3c1db8`",
    responses={406: {"model": ErrorMessage}},
    response_model=SwapItem,
    status_code=200,
)
def get_swap(uuid: str):
    try:
        db = get_sqlite_db(netid="ALL")
        resp = db.query.get_swap(uuid)
        if "error" in resp:
            raise UuidNotFoundException(resp["error"])
        return resp
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/get_swaps_for_coin/{coin}",
    description="Get swaps for a coin (all variants and tokens)",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def get_swaps_for_coin(coin: str):
    try:
        db = get_sqlite_db(netid="ALL")
        resp = db.query.get_swaps_for_coin(coin)
        if "error" in resp:
            raise UuidNotFoundException(resp["error"])
        return resp
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/get_volume_for_coin/{coin}",
    description="Get volume for a coin (all variants and tokens)",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def get_volume_for_coin(coin: str):
    try:
        db = get_sqlite_db(netid="ALL")
        resp = db.query.get_volume_for_coin(coin)
        if "error" in resp:
            raise UuidNotFoundException(resp["error"])
        return resp
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
