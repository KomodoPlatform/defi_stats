#!/usr/bin/env python3
import db.sqldb as db
from lib.pair import Pair
from util.logger import logger
from util.transform import sortdata, clean, deplatform, sumdata
from util.cron import cron
import util.memcache as memcache
import util.transform as transform
import util.validate as validate


class StatsAPI:  # pragma: no cover
    def __init__(self):
        try:
            # Load standard memcache
            self.pairs_last_trade_cache = memcache.get_pairs_last_traded()
            self.coins_config = memcache.get_coins_config()
            self.gecko_source = memcache.get_gecko_source()
            self.pg_query = db.SqlQuery()
        except Exception as e:
            logger.error(f"Failed to init Generic: {e}")

    def top_pairs(self, summaries: list):
        # TODO: Might need some transformation there
        return sortdata.top_pairs(summaries=summaries)

    def pair_summaries(self, days: int = 1, pairs_days: int = 7):
        try:
            if days == "1a":
                # TODO: disabled, we should not calc this twice
                # We should reuse generic tickers
                ticker_infos = memcache.get_tickers()
            else:
                pairs_last_trade_cache = memcache.get_pairs_last_traded()
                if days > pairs_days:
                    pairs_days = days
                pairs = sorted(
                    list(set([deplatform.pair(i) for i in pairs_last_trade_cache]))
                )
                suffix = transform.get_suffix(days)
                ticker_infos = []
                if self.gecko_source is None:
                    self.gecko_source = memcache.get_gecko_source()
                for i in pairs:
                    if validate.is_pair_priced(i, gecko_source=self.gecko_source):
                        cache_name = f"ticker_info_{i}_{suffix}_ALL"
                        d = memcache.get(cache_name)
                        if d is None:
                            d = Pair(
                                pair_str=i, pairs_last_trade_cache=pairs_last_trade_cache
                            ).ticker_info(days=days, all_variants=True)

                logger.merge(
                    f"Pair summary ticker infos ({days} days): {len(ticker_infos)}"
                )
            resp = [transform.ticker_to_statsapi_summary(i) for i in ticker_infos]
            return clean.decimal_dict_lists(resp)

        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)} Error in [StatsAPI.pair_summaries]: {e}")
            return None

    def adex_fortnite(self, days=14):
        try:
            end_time = int(cron.now_utc())
            start_time = end_time - 14 * 86400
            summaries = self.pair_summaries(days)
            liquidity = sumdata.json_key(data=summaries, key="pair_liquidity_usd")
            swaps_value = sumdata.json_key(data=summaries, key="pair_trade_value_usd")
            data = {
                "days": days,
                "swaps_count": db.SqlQuery().get_count(
                    start_time=start_time, end_time=end_time
                ),
                "swaps_value": round(float(swaps_value), 8),
                "top_pairs": self.top_pairs(summaries),
                "current_liquidity": round(float(liquidity), 8),
            }
            data = clean.decimal_dicts(data)
            return data
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)} Error in [StatsAPI.adex_fortnite]: {e}")
            return None
