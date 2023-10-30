#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from logger import logger
from cache import Cache

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
