#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from lib.generic import Generic
from typing import List
from db.sqlitedb import get_sqlite_db
from lib.cache import Cache
from models.generic import ErrorMessage
from models.stats_api import StatsApiAtomicdexIo, StatsApiSummary, StatsApiOrderbook
from util.logger import logger
import util.transform as transform
from lib.stats_api import StatsAPI
import lib

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
        cache = Cache(netid="ALL")
        tickers_data = cache.get_item(name="generic_tickers").data
        logger.info(tickers_data["combined_liquidity_usd"])
        db = get_sqlite_db(netid="ALL")
        counts = db.query.swap_counts()
        logger.info(counts)
        counts.update({"current_liquidity": tickers_data["combined_liquidity_usd"]})
        logger.info(counts)
        return counts

    except Exception as e:  # pragma: no cover
        err = {"error": f"Error in [/api/v3/stats-api/atomicdexio]: {e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get("/atomicdex_fortnight")
def atomicdex_fortnight():
    """Extra Summary Statistics over last 2 weeks"""
    try:
        # Get swaps for last 14 days
        stats = StatsAPI()
        return stats.adex_fortnite()
    except Exception as e:  # pragma: no cover
        msg = f"{type(e)} Error in [/api/v3/stats-api/atomicdex_fortnight]: {e}"
        logger.warning(msg)
        return {"error": msg}


@router.get(
    "/summary",
    description="Pair summary for last 24 hours for all pairs traded in last 7 days.",
    responses={406: {"model": ErrorMessage}},
    response_model=List[StatsApiSummary],
    status_code=200,
)
def summary():
    try:
        # Get swaps for last 14 days
        stats = StatsAPI()
        return stats.pair_summaries()
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