#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from fastapi.responses import JSONResponse
import time
from logger import logger
from models import (
    FixerRates,
    ErrorMessage
)
from cache import Cache
router = APIRouter()
cache = Cache()


# Caching

@router.on_event("startup")
@repeat_every(seconds=600)
def cache_fixer_rates():  # pragma: no cover
    try:
        cache.save.save_fixer_rates_source()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.get(
    '/fixer_io',
    description="Get usd fiat rates from data.fixer.io",
    responses={406: {"model": ErrorMessage}},
    response_model=FixerRates,
    status_code=200
)
def get_fixer_rates():
    try:
        data = cache.load.load_fixer_rates()
        if "timestamp" not in data:
            raise ValueError("No timestamp in data!")
        elif data["timestamp"] < time.time() - 900:
            raise ValueError("Data expired! No updates for > 15 mins.")
        if "error" in data:
            raise ValueError(data["error"])
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"Error in [/api/v3/rates/fixer_io]: {e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)
