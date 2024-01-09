#!/usr/bin/env python3
from fastapi import APIRouter
from util.logger import logger
from models.tickers import TickersSummary
from models.generic import ErrorMessage
from lib.cache import Cache
from util.enums import NetId

import util.transform as transform

router = APIRouter()
cache = Cache()


@router.get(
    "/summary",
    response_model=TickersSummary,
    description="24-hour price & volume for each market pair traded in last 7 days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def summary():
    try:
        cache = Cache(netid="ALL")
        data = cache.get_item(name="generic_tickers").data
        data["data"] = [transform.ticker_to_gecko(i) for i in data["data"]]
        tickers = {}
        [tickers.update({i["ticker_id"]: i}) for i in data["data"]]
        data["data"] = tickers
        return data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/tickers/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/tickers/summary]: {e}"}
