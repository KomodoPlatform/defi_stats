#!/usr/bin/env python3
from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse
from typing import List
import time
from logger import logger
from models import (
    GeckoPairsItem,
    GeckoTickersSummary,
    GeckoOrderbookItem,
    GeckoHistoricalTradesItem,
    ErrorMessage,
)
from helper import get_mm2_rpc_port
from cache import Cache
from pair import Pair
from orderbook import Orderbook
from enums import TradeType, NetId
from validate import validate_positive_numeric, validate_ticker_id

router = APIRouter()
cache = Cache()


# Gecko Endpoints
@router.get(
    "/pairs",
    response_model=List[GeckoPairsItem],
    description="a list pairs with price data traded within the week.",
)
async def gecko_pairs(netid: NetId = NetId.NETID_7777):
    try:
        return cache.load.load_gecko_pairs(netid=netid.value)
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}"}


@router.get(
    "/tickers",
    response_model=GeckoTickersSummary,
    description="24-hour price & volume for each market pair traded in last 7 days.",
)
def gecko_tickers(netid: NetId = NetId.NETID_7777):
    try:
        return cache.load.load_gecko_tickers(netid=netid.value)
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}


@router.get(
    "/orderbook/{ticker_id}",
    description="Provides current order book information for the given market pair.",
    response_model=GeckoOrderbookItem,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_orderbook(
    response: Response,
    ticker_id: str = "KMD_LTC",
    depth: int = 100,
    netid: NetId = NetId.NETID_7777,
):
    try:
        resp = {
            "ticker_id": ticker_id,
            "timestamp": f"{int(time.time())}",
            "asks": [],
            "bids": [],
            "liquidity_usd": 0,
            "total_asks_base_vol": 0,
            "total_bids_base_vol": 0,
            "total_asks_quote_vol": 0,
            "total_bids_quote_vol": 0,
            "total_asks_base_usd": 0,
            "total_bids_quote_usd": 0,
        }
        if netid.value == "all":
            for x in NetId:
                if x.value != "all":
                    gecko_pairs = cache.load.load_gecko_pairs(netid=x.value)
                    valid_tickers = [ticker["ticker_id"] for ticker in gecko_pairs]
                    validate_ticker_id(ticker_id, valid_tickers)
                    mm2_port = get_mm2_rpc_port(netid=x.value)
                    data = Orderbook(pair=Pair(ticker_id), mm2_port=mm2_port).for_pair(
                        endpoint=True, depth=depth
                    )
                    resp["asks"] += data["asks"]
                    resp["bids"] += data["bids"]
                    resp["liquidity_usd"] += data["liquidity_usd"]
                    resp["total_asks_base_vol"] += data["total_asks_base_vol"]
                    resp["total_bids_base_vol"] += data["total_bids_base_vol"]
                    resp["total_asks_quote_vol"] += data["total_asks_quote_vol"]
                    resp["total_bids_quote_vol"] += data["total_bids_quote_vol"]
                    resp["total_asks_base_usd"] += data["total_asks_base_usd"]
                    resp["total_bids_quote_usd"] += data["total_bids_quote_usd"]
            resp["bids"] = resp["bids"][:depth][::-1]
            resp["asks"] = resp["asks"][::-1][:depth]
        else:
            gecko_pairs = cache.load.load_gecko_pairs(netid=netid.value)
            valid_tickers = [ticker["ticker_id"] for ticker in gecko_pairs]
            validate_ticker_id(ticker_id, valid_tickers)
            mm2_port = get_mm2_rpc_port(netid=netid.value)
            resp = Orderbook(pair=Pair(ticker_id), mm2_port=mm2_port).for_pair(
                endpoint=True, depth=depth
            )
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)


@router.get(
    "/historical_trades/{ticker_id}",
    description="Data for completed trades for a given market pair.",
    response_model=GeckoHistoricalTradesItem,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_historical_trades(
    response: Response,
    trade_type: TradeType = "all",
    ticker_id: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = 0,
    end_time: int = 0,
    netid: NetId = NetId.NETID_7777,
):
    try:
        gecko_pairs = cache.load.load_gecko_pairs(netid=netid.value)
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
        pair = Pair(ticker_id)
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
