#!/usr/bin/env python3
from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse
from typing import List
from util.logger import logger
from lib.models_gecko import (
    GeckoPairsItem,
    GeckoTickers,
    GeckoOrderbook,
    GeckoHistoricalTrades,
)
from lib.models import ErrorMessage
from lib.cache import Cache
from lib.pair import Pair
from util.enums import TradeType, NetId
from util.validate import validate_positive_numeric, validate_ticker_id
from db.sqlitedb import get_sqlite_db_paths
from lib.generics import Generics
from util.transform import generic_orderbook_to_gecko


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
        cache = Cache(netid="ALL")
        return cache.get_item(name="gecko_pairs").data
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
        data = cache.get_item(name="gecko_tickers").data
        return data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}


@router.get(
    "/orderbook/{ticker_id}",
    description="Returns the live orderbook information for a CoinGecko compatible pair.",
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
        generics = Generics(netid=netid.value)
        data = generics.get_orderbook(ticker_id, depth)
        data = generic_orderbook_to_gecko(data)
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)


@router.get(
    "/historical_trades/{ticker_id}",
    description="Trade history for CoinGecko compatible pairs.",
    response_model=GeckoHistoricalTrades,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_historical_trades(
    response: Response,
    trade_type: TradeType = "ALL",
    ticker_id: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = 0,
    end_time: int = 0,
    netid: NetId = NetId.ALL,
):
    try:
        db_path = get_sqlite_db_paths(netid)
        cache = Cache(db_path=db_path)
        gecko_pairs = cache.load_gecko_pairs(netid=netid.value)
        valid_tickers = [ticker["ticker_id"] for ticker in gecko_pairs]
        validate_ticker_id(ticker_id, valid_tickers)
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
        return pair.historical_trades(
            trade_type=trade_type,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            netid=netid.value,
        )
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)
