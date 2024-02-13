#!/usr/bin/env python3
from fastapi import APIRouter
from util.logger import logger
from models.tickers import TickersSummary
from models.generic import ErrorMessage
from lib.cache import Cache
from util.cron import cron
from util.transform import deplatform, convert, template
import util.memcache as memcache


router = APIRouter()
cache = Cache()

# Used for stats display for pairs in Legacy desktop
@router.get(
    "/summary",
    description="24-hour price & volume for each standard market pair traded in last 7 days.",
    responses={406: {"model": ErrorMessage}},
    response_model=TickersSummary,
    status_code=200,
)
def summary():
    try:
        data = memcache.get_pair_orderbook_extended()
        volumes = memcache.get_pair_volumes_24hr()
        prices_data = memcache.get_pair_volumes_24hr()
        resp = {
            "last_update": int(cron.now_utc()),
            "pairs_count": data["pairs_count"],
            "swaps_count": data["swaps_24hr"],
            "combined_volume_usd": data["volume_usd_24hr"],
            "combined_liquidity_usd": data["combined_liquidity_usd"],
            "data": {}
        }
        for depair in data['data']:
            x = data['data'][depair]["ALL"]
            if depair in volumes["volumes"]:
                vols = volumes['volumes'][depair]["ALL"]
            else:
                vols = template.pair_trade_vol_item()
            if depair in prices_data:
                prices = prices_data[depair]["ALL"]
            else:
                prices = template.pair_prices_info()
            data.update({
                depair: convert.pair_orderbook_extras_to_gecko_tickers(x, vols, prices)
            })

        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/tickers/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/tickers/summary]: {e}"}
