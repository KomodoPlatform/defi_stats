#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from logger import logger
from cache import Cache
from db import update_master_sqlite_dbs
from enums import NetId


router = APIRouter()
cache = Cache()


@router.on_event("startup")
@repeat_every(seconds=86400)
def update_coins():  # pragma: no cover
    try:
        cache.save.save_coins()
        cache.save.save_coins_config()
        return {"result": "Updated coins"}
    except IOError as e:
        err = f"Error in [update_coins]: {e}"
        logger.warning(err)
        return {"error": err}


@router.on_event("startup")
@repeat_every(seconds=120)
def cache_gecko_data():  # pragma: no cover
    try:
        cache.save.save_gecko_source()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=600)
def cache_fixer_rates():  # pragma: no cover
    try:
        cache.save.save_fixer_rates_source()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_fixer_rates]: {e}")


@router.on_event("startup")
@repeat_every(seconds=60)
def update_dbs():
    try:
        update_master_sqlite_dbs()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_master_sqlite_dbs]: {e}")

    for netid in NetId:
        try:
            cache.save.save_gecko_tickers(netid=netid.value)
        except Exception as e:
            logger.warning(f"{type(e)} Error in [save_gecko_tickers]: {e}")

        try:
            cache.save.save_gecko_pairs(netid=netid.value)
        except Exception as e:
            logger.warning(f"{type(e)} Error in [save_gecko_pairs]: {e}")
