#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from const import MM2_DB_PATH
from logger import logger
from db import SqliteDB
from cache import Cache
from models import (
    GenericTickersInfo
)

router = APIRouter()
cache = Cache()


@router.get(
    '/summary',
    response_model=GenericTickersInfo,
    description="24-hour price & volume for each market pair traded in last 7 days."
)
def gecko_tickers():
    try:
        data = cache.load.load_gecko_tickers()
        tickers = {}
        [tickers.update({i['ticker_id']: i}) for i in data["data"]]
        data['data'] = tickers
        return data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}
