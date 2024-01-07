#!/usr/bin/env python3
import time
from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse
from typing import List
from util.logger import logger
from models.gecko import (
    GeckoPairsItem,
    GeckoTickers,
    GeckoOrderbook,
    GeckoHistoricalTrades,
)
from models.generic import ErrorMessage
from lib.cache import Cache
from lib.pair import Pair
from util.enums import TradeType, NetId
from util.validate import validate_positive_numeric
from lib.generic import Generic
from util.transform import (
    orderbook_to_gecko,
    pairs_to_gecko,
    historical_trades_to_gecko,
)
from lib.cache import load_generic_pairs

import util.transform as transform


router = APIRouter()


# Gecko Endpoints
@router.get(
    "/pairs",
    description="A list of CoinGecko compatible pairs traded within the last week.",
    response_model=List[GeckoPairsItem],
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_pairs():
    try:
        return pairs_to_gecko(load_generic_pairs())
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}"}


@router.get(
    "/tickers",
    description="24-hour price & volume for each CoinGecko compatible pair traded in last 7 days.",
    response_model=GeckoTickers,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_tickers():
    try:
        cache = Cache(netid="ALL")
        data = cache.get_item(name="generic_tickers").data
        data["data"] = [transform.ticker_to_gecko(i) for i in data["data"]]
        logger.info(data)
        return data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}


@router.get(
    "/orderbook/{ticker_id}",
    description="Returns live orderbook for a compatible pair (e.g. `KMD_LTC` ).",
    response_model=GeckoOrderbook,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_orderbook(
    response: Response,
    ticker_id: str = "KMD_LTC",
    depth: int = 100,
    netid: NetId = NetId.ALL,
):
    try:
        generic = Generic(netid=netid.value)
        data = generic.orderbook(pair_str=ticker_id, depth=depth)
        data = orderbook_to_gecko(data)
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/historical_trades/{ticker_id}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    response_model=GeckoHistoricalTrades,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_historical_trades(
    response: Response,
    trade_type: TradeType = TradeType.ALL,
    ticker_id: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = int(time.time() - 86400),
    end_time: int = int(time.time()),
    netid: NetId = NetId.ALL,
):
    try:
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate_positive_numeric(value, name)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        if trade_type not in ["all", "buy", "sell"]:
            raise ValueError("trade_type must be one of: 'all', 'buy', 'sell'")
        pair = Pair(pair_str=ticker_id)
        data = pair.historical_trades(
            trade_type=trade_type,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )
        logger.info(data)
        data["buy"] = [historical_trades_to_gecko(i) for i in data["buy"]]
        data["sell"] = [historical_trades_to_gecko(i) for i in data["sell"]]
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
