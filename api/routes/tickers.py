#!/usr/bin/env python3
from fastapi import APIRouter
from util.logger import logger
from models.tickers import TickersSummary
from models.generic import ErrorMessage
import util.memcache as memcache


router = APIRouter()


# Used for stats display for pairs in Legacy desktop
@router.get(
    "/summary",
    description="24-hour price & volume for each standard market pair traded in last 7 days.",
    responses={406: {"model": ErrorMessage}},
    response_model=TickersSummary,
    status_code=200,
)
def summary():
    try:
        # Load from cache
        return memcache.get_tickers()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/tickers/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/tickers/summary]: {e}"}
