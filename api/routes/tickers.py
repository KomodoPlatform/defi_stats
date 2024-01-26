#!/usr/bin/env python3
from decimal import Decimal
from fastapi import APIRouter
from util.logger import logger
from models.tickers import TickersSummary, TickersSummaryV2
from models.generic import ErrorMessage
from lib.cache import Cache

import util.transform as transform

router = APIRouter()
cache = Cache()


@router.get(
    "/summary",
    response_model=TickersSummary,
    description="24-hour price & volume for each market pair traded in last 7 days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def summary():
    try:
        cache = Cache(netid="ALL")
        data = cache.get_item(name="generic_tickers_old").data
        data["data"] = [transform.ticker_to_gecko(i) for i in data["data"]]
        tickers = {}
        [tickers.update({i["ticker_id"]: i}) for i in data["data"]]
        data["data"] = tickers
        return data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/tickers/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/tickers/summary]: {e}"}


@router.get(
    "/summary_v2",
    response_model=TickersSummaryV2,
    description="24-hour price & volume for each standard market pair traded in last 7 days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def summary_v2():
    try:
        cache = Cache(netid="ALL")
        data = cache.get_item(name="generic_tickers").data
        tickers = {}
        # Combine to pair without platforms
        for i in data["data"]:
            root_pair = transform.strip_pair_platforms(i["ticker_id"])
            if root_pair not in tickers:
                tickers.update({root_pair: i})
            else:
                j = tickers[root_pair]
                for key in [
                    "trades_24hr",
                    "volume_usd_24hr",
                    "liquidity_in_usd",
                    "base_volume",
                    "base_volume_usd",
                    "base_liquidity_coins",
                    "base_liquidity_usd",
                    "target_volume",
                    "quote_volume_usd",
                    "quote_liquidity_coins",
                    "quote_liquidity_usd",
                ]:
                    j[key] = transform.sum_num_str(i[key], j[key])

                if Decimal(i["last_price"]) > Decimal(j["last_price"]):
                    j["last_price"] = i["last_price"]
                    j["last_trade"] = i["last_trade"]
                    j["last_swap_uuid"] = i["last_swap_uuid"]

                if int(Decimal(i["newest_price_time"])) > int(Decimal(j["newest_price_time"])):
                    j["newest_price_time"] = i["newest_price_time"]
                    j["newest_price"] = i["newest_price"]

                if int(Decimal(i["oldest_price_time"])) < int(Decimal(j["oldest_price_time"])):
                    j["oldest_price_time"] = i["oldest_price_time"]
                    j["oldest_price"] = i["oldest_price"]

                if Decimal(i["bid"]) > Decimal(j["bid"]):
                    j["bid"] = i["bid"]

                if Decimal(i["ask"]) < Decimal(j["ask"]):
                    j["ask"] = i["ask"]

                if Decimal(i["high"]) > Decimal(j["high"]):
                    j["high"] = i["high"]

                if Decimal(i["low"]) < Decimal(j["low"]):
                    j["low"] = i["low"]

                j["price_change_24hr"] = transform.format_10f(
                    Decimal(j["newest_price"]) - Decimal(j["oldest_price"])
                )
                j["price_change_pct_24hr"] = transform.format_10f(
                    Decimal(j["newest_price"]) / Decimal(j["oldest_price"]) - 1
                )

        for i in tickers:
            tickers[i] = transform.ticker_to_gecko(tickers[i])

        data["data"] = tickers
        return data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/tickers/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/tickers/summary]: {e}"}
