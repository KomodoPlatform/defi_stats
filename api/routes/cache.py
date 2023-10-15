#!/usr/bin/env python3
import uvicorn
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from logger import logger
import models
import const
import sysrsync

router = APIRouter()
cache = models.Cache()

@router.on_event("startup")
@repeat_every(seconds=60)
def update_seednode_db():  # pragma: no cover
    '''
    This will update the MM2.db files from stats-api.atomicdex.io (a well populated seed node).
    '''
    try:
        sysrsync.run(
            source=const.MM2_DB_HOST_PATH,
            source_ssh=const.MM2_DB_HOST,
            destination=const.MM2_DB_PATH,
            options=['-avzP'],
            sync_source_contents=False
            )
        logger.info(f"Updated MM2.db from {const.MM2_DB_HOST}")
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_ticker_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=86400)
def update_coins_config():  # pragma: no cover
    try:
        cache.save.coins_config()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_coins_config]: {e}")


@router.on_event("startup")
@repeat_every(seconds=86400)
def update_coins():  # pragma: no cover
    try:
        cache.save.coins()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_coins]: {e}")
