#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import List, Dict
from const import GENERIC_PAIRS_DAYS
from lib.cache import Cache
from lib.pair import Pair
from models.generic import ErrorMessage, CoinTradeVolumes, PairTradeVolumes
from util.enums import TradeType, GroupBy
from util.exceptions import UuidNotFoundException, BadPairFormatError
from util.logger import logger
from util.transform import deplatform
import db.sqldb as db
from util.cron import cron
from util.transform import derive
import util.validate as validate

router = APIRouter()
cache = Cache()

# These endpoints not yet active. Their intent is to
# expose generic cached data which is reformatted for target specific
# endpoints. For example, the 'markets' and 'gecko' endpoints for ticker
# and pairs are 90% the same, tho might use different keys or value types
# for some items.


@router.get(
    "/get_pairs",
    description=f"Pairs traded last {GENERIC_PAIRS_DAYS} days. Ordered by mcap.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def get_pairs(remove_platforms: bool = True):
    try:
        query = db.SqlQuery()
        data = query.get_pairs(days=GENERIC_PAIRS_DAYS)
        if remove_platforms:
            data = sorted(list(set([deplatform.pair(i) for i in data])))
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/distinct",
    description="Get Unique values for a column",
    responses={406: {"model": ErrorMessage}},
    response_model=List,
    status_code=200,
)
def distinct(
    start_time: int = 0,
    end_time: int = 0,
    column: str | None = None,
    coin: str | None = None,
    pair: str | None = None,
    pubkey: str | None = None,
    gui: str | None = None,
    version: str | None = None,
    success_only: bool = True,
    failed_only: bool = False,
):
    try:
        if end_time == 0:
            end_time = int(cron.now_utc())
        query = db.SqlQuery()
        resp = sorted(
            query.get_distinct(
                start_time=start_time,
                end_time=end_time,
                column=column,
                coin=coin,
                pair=pair,
                gui=gui,
                version=version,
                failed_only=failed_only,
                success_only=success_only,
                pubkey=pubkey,
            )
        )
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/get_swaps",
    description="Swaps completed within two epoch timestamps.",
    responses={406: {"model": ErrorMessage}},
    response_model=List[db.DefiSwap] | Dict[str, List[db.DefiSwap]] | Dict,
    status_code=200,
)
def get_swaps(
    start_time: int = 0,
    end_time: int = 0,
    coin: str | None = None,
    pair_str: str | None = None,
    pubkey: str | None = None,
    gui: str | None = None,
    version: str | None = None,
    success_only: bool = True,
    failed_only: bool = False,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        query = db.SqlQuery()
        resp = query.get_swaps(
            start_time=start_time,
            end_time=end_time,
            coin=coin,
            pair_str=pair_str,
            gui=gui,
            version=version,
            failed_only=failed_only,
            success_only=success_only,
            pubkey=pubkey,
        )
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/get_swaps_for_coin/{coin}",
    description="Swaps for an exact coin matching filter.",
    responses={406: {"model": ErrorMessage}},
    response_model=List[db.DefiSwap],
    status_code=200,
)
def get_swaps_for_coin(
    coin: str,
    start_time: int = 0,
    end_time: int = 0,
    pubkey: str | None = None,
    gui: str | None = None,
    version: str | None = None,
    success_only: bool = True,
    failed_only: bool = False,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        query = db.SqlQuery()
        resp = query.get_swaps_for_coin(
            start_time=start_time,
            end_time=end_time,
            coin=coin,
            gui=gui,
            version=version,
            failed_only=failed_only,
            success_only=success_only,
            pubkey=pubkey,
        )
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/get_swaps_for_pair/{pair_str}",
    description="Swaps for an exact pair matching filter.",
    responses={406: {"model": ErrorMessage}},
    response_model=List[db.DefiSwap],
    status_code=200,
)
def get_swaps_for_pair(
    pair_str: str,
    start_time: int = 0,
    end_time: int = 0,
    pubkey: str | None = None,
    gui: str | None = None,
    version: str | None = None,
    success_only: bool = True,
    failed_only: bool = False,
    all_variants: bool = False,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())

        base, quote = derive.base_quote(pair_str)
        query = db.SqlQuery()
        resp = query.get_swaps_for_pair(
            start_time=start_time,
            end_time=end_time,
            base=base,
            quote=quote,
            gui=gui,
            version=version,
            failed_only=failed_only,
            success_only=success_only,
            pubkey=pubkey,
            all_variants=all_variants,
        )
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/historical_trades/{ticker_id}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def historical_trades(
    trade_type: TradeType = TradeType.ALL,
    ticker_id: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = 0,
    end_time: int = 0,
    variant="ALL",
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate.positive_numeric(value, name, True)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        if trade_type not in ["all", "buy", "sell"]:
            raise ValueError("trade_type must be one of: 'all', 'buy', 'sell'")
        pair = Pair(pair_str=ticker_id)

        data = pair.historical_trades(
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )

        if variant.lower() in data:
            return data[variant.lower()]
        return data
        # resp = sortdata.dict_lists(resp, "timestamp", True)
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/swap/{uuid}",
    description="Get swap info from a uuid, e.g. `82df2fc6-df0f-439a-a4d3-efb42a3c1db8`",
    responses={406: {"model": ErrorMessage}},
    response_model=db.DefiSwap,
    status_code=200,
)
def get_swap(uuid: str):
    try:
        query = db.SqlQuery()
        resp = query.get_swap(uuid=uuid)
        if "error" in resp:
            raise UuidNotFoundException(resp["error"])
        return resp
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/swap_uuids",
    description="Get swap uuids for a pair (e.g. `KMD_LTC`).",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def swap_uuids(
    start_time: int = 0,
    end_time: int = 0,
    coin: str | None = None,
    pair: str | None = None,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        query = db.SqlQuery()
        uuids = query.swap_uuids(
            start_time=start_time, end_time=end_time, coin=coin, pair=pair
        )
        if coin is not None:
            return {
                "coin": coin,
                "start_time": start_time,
                "end_time": end_time,
                "variants": list(uuids.keys()),
                "swap_count": len(uuids["ALL"]),
                "swap_uuids": uuids,
            }
        elif pair is not None:
            return {
                "pair": pair,
                "start_time": start_time,
                "end_time": end_time,
                "variants": list(uuids.keys()),
                "swap_count": len(uuids["ALL"]),
                "swap_uuids": uuids,
            }
        return {
            "start_time": start_time,
            "end_time": end_time,
            "swap_count": len(uuids),
            "swap_uuids": uuids,
        }
    except BadPairFormatError as e:
        err = {"error": e.name, "message": e.msg}
        return JSONResponse(status_code=e.status_code, content=err)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/coin_trade_volumes_usd",
    description="Trade volumes for each coin over the selected time period.",
    responses={406: {"model": ErrorMessage}},
    response_model=CoinTradeVolumes,
    status_code=200,
)
def coin_trade_volumes_usd(
    start_time: int = 0,
    end_time: int = 0,
    pubkey: str | None = None,
    gui: str | None = None,
    version: str | None = None,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        query = db.SqlQuery()
        volumes = query.coin_trade_volumes(
            start_time=start_time,
            end_time=end_time,
        )
        return query.coin_trade_volumes_usd(volumes=volumes)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/pair_trade_volumes_usd",
    description="Trade volumes for each pair over the selected time period.",
    responses={406: {"model": ErrorMessage}},
    response_model=PairTradeVolumes,
    status_code=200,
)
def pair_trade_volumes_usd(
    start_time: int = 0,
    end_time: int = 0,
    pubkey: str | None = None,
    gui: str | None = None,
    coin: str | None = None,
    version: str | None = None,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        query = db.SqlQuery()
        volumes = query.pair_trade_volumes(
            start_time=start_time,
            end_time=end_time,
            version=version,
            pubkey=pubkey,
            coin=coin,
            gui=gui,
        )
        return query.pair_trade_volumes_usd(volumes=volumes)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


# Unfinished phase 3 endpoints below


@router.get(
    "/last_traded/{category}",
    description="Trade volumes grouped by category over the selected time period.",
    responses={406: {"model": ErrorMessage}},
    # response_model=PairTradeVolumes,
    status_code=200,
)
def last_traded(category: GroupBy, min_swaps: int = 0):
    try:
        query = db.SqlQuery()
        match category:
            case GroupBy.pair:
                return query.pair_last_trade()
            case GroupBy.gui:
                return query.gui_last_traded()
            case GroupBy.coin:
                return query.coin_last_traded()
            case GroupBy.ticker:
                return query.ticker_last_traded()
            case GroupBy.platform:
                return query.platform_last_traded()
            case GroupBy.pubkey:
                return query.pubkey_last_traded()
            case GroupBy.version:
                return query.version_last_traded()
            case _:
                return {"error": "Invalid selection for `group_by`"}
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
