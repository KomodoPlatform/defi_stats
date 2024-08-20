#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from util.cron import cron
from util.logger import logger
from models.generic import ErrorMessage, ApiIds
from util.files import Files
import util.memcache as memcache
from util.transform import derive

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
        data = memcache.get(cache_name)
        if data is None:
            data = {"timestamp": int(cron.now_utc()), "ids": {}}
            coins_config = memcache.get_gecko_source()
            for coin in coins_config:
                data["ids"].update({coin: coins_config[coin]["coingecko_id"]})
            memcache.update(cache_name, data, 600)
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


@router.get(
    "/volumes_alltime",
    description="",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def volumes_24hr():
    return memcache.get_coin_volumes_alltime()


@router.get(
    "/top_coins",
    description="Coins with highest swap counts and usd volumes",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def top_coins():
    vols = memcache.get_coin_volumes_alltime()
    return {
        "top_swaps": derive.top_coin_by_swap_counts(vols),
        "top_volumes": derive.top_coin_by_volume(vols)
    }
