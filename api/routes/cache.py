#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from logger import logger
from cache import Cache
from db import remove_overlaps, update_master_sqlite_dbs
from logger import logger
from enums import NetId
from const import (
    PROJECT_ROOT_PATH,
    LOCAL_MM2_DB_PATH_7777,
    LOCAL_MM2_DB_PATH_8762,
    LOCAL_MM2_DB_BACKUP_7777,
    LOCAL_MM2_DB_BACKUP_8762,
    MM2_DB_PATHS
)


router = APIRouter()
cache = Cache()


@router.on_event("startup")
@repeat_every(seconds=86400)
def update_coins_config():  # pragma: no cover
    try:
        cache.save.save_coins_config()
        return {"result": "Updated coins_config.json"}
    except Exception as e:
        err = f"Error in [update_coins_config]: {e}"
        logger.warning(err)
        return {"error": err}


@router.on_event("startup")
@repeat_every(seconds=86400)
def update_coins():  # pragma: no cover
    try:
        cache.save.save_coins()
        return {"result": "Updated coins.json"}
    except Exception as e:
        err = f"Error in [update_coins]: {e}"
        logger.warning(err)
        return {"error": err}

# Gecko Caching


@router.on_event("startup")
@repeat_every(seconds=120)
def cache_gecko_data():  # pragma: no cover
    try:
        cache.save.save_gecko_source()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=120)
def cache_gecko_pairs():  # pragma: no cover
    remove_overlaps()
    for netid in NetId:
        try:
            cache.save.save_gecko_pairs(netid=netid.value)
        except Exception as e:
            logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=180)
def cache_gecko_tickers():  # pragma: no cover
    remove_overlaps()
    for netid in NetId:
        try:
            cache.save.save_gecko_tickers(netid=netid.value)
        except Exception as e:
            logger.warning(f"{type(e)} Error in [cache_gecko_tickers]: {e}")


@router.on_event("startup")
@repeat_every(seconds=60)
def update_dbs():
    try:
        result = update_master_sqlite_dbs()
        # logger.info(result)
        return result
    except Exception as e:
        err = f"Error in [update_master_dbs]: {e}"
        logger.warning(err)
        return {"error": err}
