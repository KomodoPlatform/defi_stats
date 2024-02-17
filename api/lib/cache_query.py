#!/usr/bin/env python3
from util.logger import logger, timed
from util.transform import invert, derive, template
import db.sqldb as db
import util.defaults as default
import util.memcache as memcache


from util.transform import deplatform


class CacheQuery:
    def __init__(self) -> None:
        #self.coins_config = memcache.get_coins_config()
        #self.pairs_last_trade_cache = memcache.get_pair_last_traded()
        #self.pairs_last_trade_24hr_cache = memcache.get_pair_last_traded_24hr()
        #self.gecko_source = memcache.get_gecko_source()
        #self.pg_query = db.SqlQuery()
        pass


    @timed
    def pair_price_24hr(self, pair_str: str):  # pragma: no cover
        try:
            # Add 24hr prices
            suffix = derive.suffix(1)
            base, quote = derive.base_quote(pair_str=pair_str)
            data = template.pair_prices_info(suffix=suffix)
            depair = deplatform.pair(pair_str)
            prices_data = memcache.get_pair_prices_24hr()
            if prices_data is not None:
                if depair in prices_data:
                    if pair_str in prices_data[depair]:
                        v = prices_data[depair][pair_str]
                        data.update(v)
                elif invert.pair(depair) in prices_data:
                    if invert.pair(pair_str) in prices_data[invert.pair(depair)]:
                        v = prices_data[invert.pair(depair)][invert.pair(pair_str)]
                        data.update(v)
            return default.result(
                data=data,
                msg=f"{pair_str} complete",
                loglevel="cached",
                ignore_until=5,
            )
        except Exception as e:
            msg = f"{pair_str} failed: {e}!"
            try:
                data = template.pair_prices_info(suffix=suffix)
                msg += " Returning template!"
            except Exception as e:
                data = {"error": f"{msg}: {e}"}
            return default.result(data=data, msg=msg, loglevel="warning", ignore_until=0)


cache_query = CacheQuery()