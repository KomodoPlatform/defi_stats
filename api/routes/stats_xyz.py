#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from decimal import Decimal
from datetime import datetime, timedelta
from const import MARKETS_PAIRS_DAYS
from lib.generic import Generic
from lib.pair import Pair
from models.generic import ErrorMessage
from models.markets import MarketsAtomicdexIo
from util.enums import TradeType
from util.logger import logger
from util.exceptions import BadPairFormatError
from util.transform import sortdata, deplatform
import db
import util.cron as cron
import util.helper as helper
import util.memcache as memcache
import util.templates as template
import util.transform as transform
import util.validate as validate


clean = transform.Clean()
router = APIRouter()


# TODO: Move to new DB
@router.get(
    "/atomicdexio",
    description="Returns atomic swap counts over a variety of periods",
    response_model=MarketsAtomicdexIo,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def atomicdex_info_api():
    # TODO: Use new DB
    query = db.SqlQuery()
    return query.swap_counts()


# New endpoint
@router.get(
    "/current_liquidity",
    description="Global liquidity on the orderbook for all pairs.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def current_liquidity():
    try:
        data = memcache.get_tickers()
        return {"current_liquidity": data["combined_liquidity_usd"]}

    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}"}


@router.get(
    "/fiat_rates",
    description="Coin prices in USD (where available)",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def fiat_rates():
    return memcache.get_gecko_source()


@router.get(
    "/orderbook/{market_pair}",
    description="Get Orderbook for a market pair in `KMD_LTC` format.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def orderbook(market_pair: str = "KMD_LTC", depth: int = 100):
    try:
        generic = Generic()
        return generic.orderbook(pair_str=market_pair, depth=depth, no_thread=True)
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/pairs_last_trade",
    description="Returns last trade info for all pairs matching the filter",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def pairs_last_traded(
    start_time: int = 0,
    end_time: int = int(cron.now_utc()),
) -> list:
    data = memcache.get_last_traded()
    filtered_data = []
    for i in data:
        if data[i]["last_swap_time"] > start_time:
            if data[i]["last_swap_time"] < end_time:
                data[i].update({"pair": i})
                filtered_data.append(data[i])
    return filtered_data


# Migrated from https://stats.testchain.xyz/api/v1/summary
@router.get(
    "/summary",
    description=f"24-hour price & volume for each pair traded in last {MARKETS_PAIRS_DAYS} days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def summary():
    try:
        data = memcache.get_tickers()
        resp = []
        for i in data["data"]:
            resp.append(transform.ticker_to_xyz_summary(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats_xyz/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats_xyz/tickers]: {e}"}


# Migrated from https://stats.testchain.xyz/api/v1/summary_for_ticker/KMD
@router.get(
    "/summary_for_ticker/{coin}",
    description="24h price & volume for market pairs with a specific coin traded in last 7 days",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def summary_for_ticker(coin: str = "KMD"):
    # TODO: Segwit not merged in this endpoint yet
    try:
        last_traded = memcache.get_last_traded()
        resp = memcache.get_tickers()
        new_data = []
        for i in resp["data"]:
            if coin in [i["base_currency"], i["quote_currency"]]:
                if i["last_swap_time"] == 0:
                    if i["ticker_id"] in last_traded:
                        i = i | last_traded[i["ticker_id"]]
                new_data.append(transform.to_summary_for_ticker_xyz_item(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats_xyz/summary_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats_xyz/summary_for_ticker]: {e}"}


@router.get(
    "/swaps24/{coin}",
    description="Total swaps involving a specific coin (e.g. `KMD`) in the last 24hrs.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def swaps24(coin: str = "KMD") -> dict:
    # TODO: Lower than xyz source. Is it combined?
    try:
        data = memcache.get_tickers()
        trades = 0
        for i in data["data"]:
            if coin in [i["base_currency"], i["quote_currency"]]:
                trades += int(i["trades_24hr"])
        return {"ticker": coin, "swaps_amount_24h": trades}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats_xyz/swaps24]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats_xyz/swaps24]: {e}"}


@router.get(
    "/ticker",
    description="Simple last price and liquidity for each market pair, traded in last 7 days.",
)
def ticker():
    try:
        data = memcache.get_tickers()
        resp = []
        for i in data["data"]:
            resp.append(transform.ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats_xyz/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats_xyz/ticker]: {e}"}


@router.get(
    "/ticker_for_ticker",
    description="Simple last price and liquidity for each market pair for a specific ticker.",
)
def ticker_for_ticker(ticker):
    try:
        data = memcache.get_tickers()
        resp = []
        for i in data["data"]:
            if ticker in [i["base_currency"], i["quote_currency"]]:
                resp.append(transform.ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats_xyz/ticker_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats_xyz/ticker_for_ticker]: {e}"}


@router.get(
    "/tickers_summary",
    description="Total swaps and volume involving for each active ticker in the last 24hrs.",
)
def tickers_summary():
    try:
        data = memcache.get_tickers()
        resp = {}
        for i in data["data"]:
            base = i["base_currency"]
            rel = i["quote_currency"]
            for ticker in [base, rel]:
                if ticker not in resp:
                    resp.update({ticker: {"trades_24h": 0, "volume_24h": 0}})
                resp[ticker]["trades_24h"] += int(i["trades_24hr"])
                if ticker == base:
                    resp[ticker]["volume_24h"] += Decimal(i["base_volume"])
                elif ticker == rel:
                    resp[ticker]["volume_24h"] += Decimal(i["quote_volume"])
        resp = clean.decimal_dicts(resp)
        with_action = {}  # has a recent swap
        tickers = list(resp.keys())
        tickers.sort()
        for ticker in tickers:
            tickers = list(resp[ticker])
            if resp[ticker]["trades_24h"] > 0:
                with_action.update({ticker: resp[ticker]})
        return with_action
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/stats_xyz/swaps24]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/stats_xyz/swaps24]: {e}"}


@router.get(
    "/trades/{pair_str}/{days_in_past}",
    description="Trades for the last 'x' days for a pair in `KMD_LTC` format.",
)
def trades(
    pair_str: str = "KMD_LTC", days_in_past: int | None = None
):
    try:
        for value, name in [(days_in_past, "days_in_past")]:
            validate.positive_numeric(value, name)
        if days_in_past > 90:
            return {"error": "Maximum value for 'days_in_past' is '90'. Try again."}
        start_time = int(cron.now_utc() - 86400 * days_in_past)
        end_time = int(cron.now_utc())

        pair = Pair(pair_str=pair_str)
        data = pair.historical_trades(
            start_time=start_time,
            end_time=end_time,
        )["ALL"]
        resp = data["buy"] + data["sell"]
        resp = sortdata.dict_lists(resp, "timestamp", True)
        return resp
    except BadPairFormatError as e:
        err = f"{type(e)} Error in [/api/v3/stats_xyz/trades]: {e.msg}"
        logger.warning(err)
    except Exception as e:
        err = f"{type(e)} Error in [/api/v3/stats_xyz/trades]: {e}"
        logger.warning(err)
    return JSONResponse(status_code=400, content=err)


# Migrated from https://stats.testchain.xyz/api/v1/usd_volume_24h
@router.get(
    "/usd_volume_24hr",
    description="Volume (in USD) traded in last 24hrs.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def usd_volume_24h():
    try:
        data = memcache.get_tickers()
        return {"usd_volume_24h": data["combined_volume_usd"]}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}"}


@router.get(
    "/volumes_ticker/{coin}/{days_in_past}",
    description="Daily coin volume (e.g. `KMD, KMD-BEP20, KMD-ALL`) traded last 'x' days.",
)
def volumes_history_ticker(
    coin="KMD", days_in_past=1, trade_type: TradeType = TradeType.ALL
):
    # TODO: Use new DB
    volumes_dict = {}
    query = db.SqlQuery()
    # Individual tickers only, no merge except segwit
    stripped_coin = deplatform.coin(coin)
    variants = helper.get_coin_variants(coin, segwit_only=True)
    for i in range(0, int(days_in_past)):
        d = datetime.today() - timedelta(days=i)
        d_str = d.strftime("%Y-%m-%d")
        day_ts = int(int(d.strftime("%s")) / 86400) * 86400
        start_time = int(day_ts)
        end_time = int(day_ts) + 86400
        volumes = query.coin_trade_volumes(start_time=start_time, end_time=end_time)
        data = query.coin_trade_volumes_usd(volumes)
        volumes_dict[d_str] = template.volumes_ticker()
        for variant in variants:
            if stripped_coin in data["volumes"]:
                if variant in data["volumes"][stripped_coin]:
                    volumes_dict[d_str] = (
                        volumes_dict[d_str] | data["volumes"][stripped_coin][variant]
                    )
    return volumes_dict
