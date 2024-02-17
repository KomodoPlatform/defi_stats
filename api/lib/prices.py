#!/usr/bin/env python3
from lib.pair import Pair
from util.cron import cron
from util.logger import timed
from util.transform import derive
import util.defaults as default
import util.memcache as memcache


@timed
def pair_prices(days=1, from_memcache: bool = False):
    try:
        # Get cached data
        suffix = derive.suffix(days)
        coins_config = memcache.get_coins_config()
        pairs_last_trade_cache = memcache.get_pair_last_traded()
        pair_vols = memcache.get_pair_volumes_24hr()
        # Filter out pairs older than requested time
        ts = cron.now_utc() - days * 86400
        pairs = derive.pairs_traded_since(ts, pairs_last_trade_cache)
        prices_data = {
            i: Pair(
                pair_str=i,
                pairs_last_trade_cache=pairs_last_trade_cache,
                coins_config=coins_config,
            ).get_pair_prices_info(days)
            for i in pairs
        }
        resp = {}
        for depair in prices_data:
            resp.update({depair: {}})
            variants = sorted(list(prices_data[depair].keys()))
            for variant in variants:
                if prices_data[depair][variant]["newest_price_time"] != 0:
                    resp[depair].update({variant: prices_data[depair][variant]})
                    resp[depair][variant].update(
                        {f"trades_{suffix}": 0, "trade_volume_usd": 0}
                    )
                    if depair in pair_vols["volumes"]:
                        if variant in pair_vols["volumes"][depair]:
                            resp[depair][variant].update(
                                {
                                    f"trades_{suffix}": pair_vols["volumes"][depair][
                                        variant
                                    ][f"trades_{suffix}"],
                                    "trade_volume_usd": pair_vols["volumes"][depair][
                                        variant
                                    ]["trade_volume_usd"],
                                }
                            )

        msg = f"[pair_prices_{suffix}] update loop complete"
        return default.result(resp, msg, loglevel="calc", ignore_until=3)
    except Exception as e:  # pragma: no cover
        msg = f"[pair_prices_{suffix}] failed! {e}"
        return default.error(e, msg)
