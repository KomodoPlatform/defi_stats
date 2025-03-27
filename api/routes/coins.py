#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import List, Dict
from util.cron import cron
from util.logger import logger
from models.generic import ErrorMessage, ApiIds
import db.sqldb as db
from util.files import Files
import util.memcache as memcache
from util.transform import derive

router = APIRouter()
files = Files()
query = db.SqlQuery()

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


@router.get(
    "/get_swaps_for_coin/{coin}",
    description="Swaps for an coin matching filter.",
    responses={406: {"model": ErrorMessage}},
    response_model=List[db.DefiSwap],
    status_code=200,
)
def get_swaps_for_coin(
    coin_str: str,
    start_time: int = 0,
    end_time: int = 0,
    pubkey: str | None = None,
    gui: str | None = None,
    version: str | None = None,
    success_only: bool = True,
    failed_only: bool = False,
    all_variants: bool = False,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())

        resp = query.get_swaps_for_coin(
            start_time=start_time,
            end_time=end_time,
            coin=coin_str,
            gui=gui,
            version=version,
            failed_only=failed_only,
            success_only=success_only,
            pubkey=pubkey,
            all_variants=all_variants,
        )
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
