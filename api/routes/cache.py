#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from logger import logger
import models

router = APIRouter()
cache = models.Cache()


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
