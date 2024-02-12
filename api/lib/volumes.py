#!/usr/bin/env python3
from decimal import Decimal
from util.logger import timed
from util.transform import deplatform, invert, template

import util.defaults as default
import util.memcache as memcache
import util.transform as transform


@timed
def pair_volume_24hr_cache(pair_str: str = "KMD_LTC"):
    try:
        # Add 24hr trades and volume
        data = {"volume_usd_24hr": 0, "trades_24hr": 0}
        depair = deplatform.pair(pair_str)
        volume_data = memcache.get_pair_volumes_24hr()
        if depair in volume_data["volumes"]:
            if pair_str in volume_data["volumes"][depair]:
                v = volume_data["volumes"][depair][pair_str]
                data.update(
                    {
                        "volume_usd_24hr": Decimal(v["trade_volume_usd"]),
                        "trades_24hr": int(v["swaps"]),
                    }
                )
        elif invert.pair(depair) in volume_data["volumes"]:
            if invert.pair(pair_str) in volume_data["volumes"][invert.pair(pair_str)]:
                v = volume_data["volumes"][invert.pair(depair)][invert.pair(pair_str)]
                data.update(
                    {
                        "volume_usd_24hr": Decimal(v["trade_volume_usd"]),
                        "trades_24hr": int(v["swaps"]),
                    }
                )
        ignore_until = 3
        vol = transform.format_10f(data["volume_usd_24hr"])
        if data["trades_24hr"] > 3:
            ignore_until = 0
        return default.result(
            data=data,
            msg=f"{pair_str} volume_24hr: {vol} ({data['trades_24hr']} swaps)",
            loglevel="cached",
            ignore_until=ignore_until,
        )
    except Exception as e:  # pragma: no cover
        msg = f"Pair.get_pair_volume_24hr_cache {pair_str} failed: {e}!"
        try:
            data = template.orderbook(pair_str)
            msg += " Returning template!"
        except Exception as e:  # pragma: no cover
            data = {"error": f"{msg}: {e}"}
        return default.result(data=data, msg=msg, loglevel="warning", ignore_until=0)
