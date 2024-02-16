#!/usr/bin/env python3
from decimal import Decimal
from util.logger import timed
from util.transform import deplatform, invert, template

import util.defaults as default
import util.memcache as memcache


@timed
def pair_volume_24hr_cache(pair_str: str = "KMD_LTC"):  # pragma: no cover
    try:
        # Add 24hr trades and volume
        v = None
        depair = deplatform.pair(pair_str)
        volume_data = memcache.get_pair_volumes_24hr()
        
        if depair in volume_data["volumes"]:
            if pair_str in volume_data["volumes"][depair]:
                v = volume_data["volumes"][depair][pair_str]
                
        elif invert.pair(depair) in volume_data["volumes"]:
            if invert.pair(pair_str) in volume_data["volumes"][invert.pair(depair)]:
                v = volume_data["volumes"][invert.pair(depair)][invert.pair(pair_str)]
        if v is None:
            vol = 0
            swaps = 0
        else:
            vol = Decimal(v["trade_volume_usd"])
            swaps = int(v["swaps"])
        data = {"volume_usd_24hr": vol, "trades_24hr": swaps}
        ignore_until = 3
        if data["trades_24hr"] > 1:
            ignore_until = 0
        msg = f"{pair_str} volume_24hr: {data['volume_usd_24hr']} ({data['trades_24hr']} swaps)"
        return default.result(
            data=data,
            msg=msg,
            loglevel="cached",
            ignore_until=ignore_until,
        )
    except Exception as e:
        msg = f"Pair.get_pair_volume_24hr_cache {pair_str} failed: {e}!"
        try:
            data = template.orderbook(pair_str, suffix="24hr")
            msg += " Returning template!"
        except Exception as e:
            data = {"error": f"{msg}: {e}"}
        return default.result(data=data, msg=msg, loglevel="warning", ignore_until=0)


@timed
def pair_volume_14d_cache(pair_str: str = "KMD_LTC"):  # pragma: no cover
    try:
        # Add 24hr trades and volume
        v = None
        depair = deplatform.pair(pair_str)
        volume_data = memcache.get_pair_volumes_14d()
        if depair in volume_data["volumes"]:
            if pair_str in volume_data["volumes"][depair]:
                v = volume_data["volumes"][depair][pair_str]
        elif invert.pair(depair) in volume_data["volumes"]:
            if invert.pair(pair_str) in volume_data["volumes"][invert.pair(pair_str)]:
                v = volume_data["volumes"][invert.pair(depair)][invert.pair(pair_str)]
        if v is None:
            vol = 0
            swaps = 0
        else:
            vol = Decimal(v["trade_volume_usd"])
            swaps = int(v["swaps"])
        data = {"volume_usd_14d": vol, "trades_14d": swaps}
        ignore_until = 3
        if data["trades_14d"] > 3:
            ignore_until = 0
        msg = f"{pair_str} volume_14d: {data['volume_usd_14d']} ({data['trades_14d']} swaps)"
        return default.result(
            data=data,
            msg=msg,
            loglevel="cached",
            ignore_until=ignore_until,
        )
    except Exception as e:
        msg = f"Pair.get_pair_volume_14d_cache {pair_str} failed: {e}!"
        try:
            data = template.orderbook(pair_str, suffix="14d")
            msg += " Returning template!"
        except Exception as e:
            data = {"error": f"{msg}: {e}"}
        return default.result(data=data, msg=msg, loglevel="warning", ignore_until=0)
