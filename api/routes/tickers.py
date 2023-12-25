#!/usr/bin/env python3
from fastapi import APIRouter
from util.logger import logger
from lib.cache import Cache
from lib.models import GenericTickersInfo
from util.enums import NetId

router = APIRouter()
cache = Cache()


@router.get(
    '/summary',
    response_model=GenericTickersInfo,
    description="24-hour price & volume for each market pair traded in last 7 days."
)
def summary(netid: NetId = NetId.ALL):
    try:
        data = cache.load_gecko_tickers(netid=netid.value)
        tickers = {}
        [tickers.update({i['ticker_id']: i}) for i in data["data"]]
        data['data'] = tickers
        return data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}
