#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import List
from db.sqlitedb import get_sqlite_db
from lib.cache import Cache
from models.generic import ErrorMessage
from models.stats_api import StatsApiAtomicdexIo, StatsApiSummary
from util.logger import logger
from lib.stats_api import StatsAPI

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
        logger.warning(f"{type(e)} Error in [stats-api/summary]: {e}")
        return {"error": f"{type(e)} Error in [stats-api/atomicdexio]: {e}"}
