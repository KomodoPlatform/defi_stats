#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from util.cron import cron
from util.logger import logger
from models.generic import ErrorMessage, ApiIds
from util.files import Files
import util.memcache as memcache

router = APIRouter()
files = Files()


@router.get(
    "/api_ids/gecko",
    description="Get API ids from 3rd party providers.",
    responses={406: {"model": ErrorMessage}},
    response_model=ApiIds,
    status_code=200,
)
def get_gecko_ids():
    try:
        cache_name = 'gecko_api_ids'
        data = {"timestamp": int(cron.now_utc()), "ids": {}}
        coins_config = memcache.get_gecko_source()
        for coin in coins_config:
            data["ids"].update({coin: coins_config[coin]["coingecko_id"]})
        return data
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/config",
    description="",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def config():
    return memcache.get_coins_config()


@router.get(
    "/raw",
    description="",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def raw():
    return memcache.get_coins()


@router.get(
    "/volumes_24hr",
    description="",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def volumes_24hr():
    return memcache.get_coin_volumes_24hr()
