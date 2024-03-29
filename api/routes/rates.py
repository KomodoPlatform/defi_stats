#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from util.logger import logger
from models.generic import FixerRates, ErrorMessage
from lib.cache import Cache

router = APIRouter()
cache = Cache()


@router.get(
    "/fixer_io",
    description="Get usd fiat rates from data.fixer.io",
    responses={406: {"model": ErrorMessage}},
    response_model=FixerRates,
    status_code=200,
)
def get_fixer_rates():
    try:
        cache = Cache()
        data = cache.get_item(name="fixer_rates").data
        if data is not None:
            return data
        return {}
        '''
            if "timestamp" not in data:
                raise ValueError("No timestamp in data!")
            elif data["timestamp"] < cron.now_utc() - 900:
                raise ValueError("Data expired! No updates for > 15 min")
            if "error" in data:
                raise ValueError(data["error"])
            return data
        '''
    except Exception as e:  # pragma: no cover
        err = {"error": f"Error in [/api/v3/rates/fixer_io]: {e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
