#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
import time
from util.logger import logger
from lib.models import ErrorMessage, ApiIds
from util.files import Files
from lib.cache_item import CacheItem

router = APIRouter()
files = Files()


@router.get(
    '/api_ids/gecko',
    description="Get API ids from 3rd party providers.",
    responses={406: {"model": ErrorMessage}},
    response_model=ApiIds,
    status_code=200
)
def get_gecko_ids():
    try:
        data = {
            "timestamp": int(time.time()),
            "ids": {}
        }
        coins_config = CacheItem('coins_config').data
        for coin in coins_config:
            data["ids"].update({
                coin: coins_config[coin]["coingecko_id"]
            })
        return data
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)
