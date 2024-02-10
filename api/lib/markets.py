#!/usr/bin/env python3
from const import MARKETS_PAIRS_DAYS
from lib.pair import Pair
from lib.coins import get_segwit_coins
from util.logger import timed, logger
from util.transform import sortdata, derive, deplatform, invert, merge
from util.cron import cron
import util.defaults as default
import util.memcache as memcache


class Markets:
    def __init__(self) -> None:
        try:
            self.netid = 8762
            self.segwit_coins = [i for i in get_segwit_coins()]
            self.coins_config = memcache.get_coins_config()
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Markets: {e}")

    @timed
    def tickers(self, trades_days: int = 1, pairs_days: int = MARKETS_PAIRS_DAYS):
        try:
            # Todo: Set this cache up for temporal filtering
            tickers = memcache.get_tickers()
            return tickers
        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default.error(e, msg)

    # TODO: Cache this
    def trades(self, pair_str: str, days_in_past: int = 1, all_variants: bool = False):
        try:
            pairs_last_trade_cache = memcache.get_pair_last_traded()
            start_time = int(cron.now_utc() - 86400 * days_in_past)
            end_time = int(cron.now_utc())
            data = Pair(
                pair_str=pair_str, pairs_last_trade_cache=pairs_last_trade_cache
            ).historical_trades(
                start_time=start_time,
                end_time=end_time,
            )
            resp = []
            base, quote = derive.base_quote(pair_str)
            if all_variants:
                resp = merge.trades(resp, data["ALL"])
            else:
                variants = derive.pair_variants(
                    pair_str=pair_str,
                    segwit_only=True,
                    coins_config=self.coins_config
                )
                for v in variants:
                    resp = merge.trades(resp, data[v])
            return sortdata.dict_lists(resp, "timestamp", reverse=True)

        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default.error(e, msg)

    def get_pair_volume_from_cache(self, pair_str, merge_segwit=True):
        trades = 0
        volume = 0
        if merge_segwit:
            variants = derive.pair_variants(pair_str, segwit_only=True)
        else:
            variants = [pair_str]
        pair_volumes = memcache.get_pair_volumes_24hr()
        if pair_volumes is not None:
            depair = deplatform.pair(pair_str)
            pair_vols = pair_volumes["volumes"]
            if depair in pair_vols:
                # only merge segwit, /markets wants protocols
                for variant in variants:
                    if variant in pair_vols[depair]:
                        trades = pair_vols[depair][variant]["swaps"]
                        volume = pair_vols[depair][variant]["trade_volume_usd"]
            elif invert.pair(depair) in pair_vols:
                inv_depair = invert.pair(depair)
                # only merge segwit, /markets wants protocols
                variants = [invert.pair(i) for i in variants]
                for variant in variants:
                    if variant in pair_vols[inv_depair]:
                        trades = pair_vols[inv_depair][variant]["swaps"]
                        volume = pair_vols[inv_depair][variant]["trade_volume_usd"]
        return {"trades_24hr": trades, "volume_usd_24hr": volume, "variants": variants}
