#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
import util.cron as cron
from util.logger import logger
from models.generic import ErrorMessage, ApiIds
from util.files import Files
import util.memcache as memcache

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
            "timestamp": int(cron.now_utc()),
            "ids": {}
        }
        coins_config = memcache.get_gecko_source()
        for coin in coins_config:
            data["ids"].update({
                coin: coins_config[coin]["coingecko_id"]
            })
        return data
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
