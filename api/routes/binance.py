#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from util.logger import logger
from models.generic import ErrorMessage
from lib.cache import Cache
from lib.external import BinanceAPI

router = APIRouter()
cache = Cache()


@router.get(
    "/ticker_price",
    description="Get binance ticker prices",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def get_ticker_price():
    try:
        binance = BinanceAPI()
        return binance.ticker_price()
    except Exception as e:  # pragma: no cover
        err = {"error": f"Error in [/api/v3/binance/ticker/price]: {e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
