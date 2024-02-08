#!/usr/bin/env python3
from util.logger import logger, timed
from util.transform import clean, sortdata, sumdata, derive, convert, merge
import db.sqldb as db
from util.cron import cron
import util.defaults as default
import util.helper as helper
import util.memcache as memcache
import util.transform as transform
from lib.pair import Pair
from util.transform import deplatform


class CacheCalc:
    def __init__(self) -> None:
        self.coins_config = memcache.get_coins_config()
        self.pairs_last_trade_cache = memcache.get_pairs_last_traded()
        self.gecko_source = memcache.get_gecko_source()
        self.pg_query = db.SqlQuery()

    # FOUNDATIONAL CACHE
    @timed
    def pairs_last_traded(self):
        try:
            if self.gecko_source is None:
                self.gecko_source = memcache.get_gecko_source()
            data = self.pg_query.pair_last_trade()
            price_status_dict = derive.price_status_dict(data.keys(), self.gecko_source)
            for i in data:
                data[i] = clean.decimal_dicts(data[i])
                data[i].update(
                    {"priced": helper.get_pair_priced_status(i, price_status_dict)}
                )

            msg = "pairs_last_traded complete!"
            return default.result(data, msg, loglevel="loop")
        except Exception as e:  # pragma: no cover
            msg = f"pairs_last_traded failed! {e}"
            logger.warning(msg)

    @timed
    def coin_volumes_24hr(self):
        try:
            if self.gecko_source is None:
                self.gecko_source = memcache.get_gecko_source()
            vols = self.pg_query.coin_trade_volumes()
            vols_usd = self.pg_query.coin_trade_volumes_usd(vols, self.gecko_source)
            for coin in vols_usd["volumes"]:
                for variant in vols_usd["volumes"][coin]:
                    vols_usd["volumes"][coin][variant] = clean.decimal_dicts(
                        vols_usd["volumes"][coin][variant]
                    )
            vols_usd = clean.decimal_dicts(vols_usd)
            msg = "coin_volumes_24hr complete!"
            return default.result(vols_usd, msg, loglevel="loop")
        except Exception as e:  # pragma: no cover
            msg = f"coin_volumes_24hr failed! {e}"
            logger.warning(msg)

    @timed
    def pair_volumes_24hr(self):
        try:
            if self.gecko_source is None:
                self.gecko_source = memcache.get_gecko_source()
            vols = self.pg_query.pair_trade_volumes()
            vols_usd = self.pg_query.pair_trade_volumes_usd(vols, self.gecko_source)
            for pair_str in vols_usd["volumes"]:
                for variant in vols_usd["volumes"][pair_str]:
                    vols_usd["volumes"][pair_str][variant] = clean.decimal_dicts(
                        vols_usd["volumes"][pair_str][variant]
                    )
            vols_usd = clean.decimal_dicts(vols_usd)
            msg = "pair_volumes_24hr complete!"
            return default.result(vols_usd, msg, loglevel="loop")
        except Exception as e:  # pragma: no cover
            msg = f"pair_volumes_24hr failed! {e}"
            logger.warning(msg)

    @timed
    def orderbook_extended(
        self,
        trades_days: int = 1,
        pairs_days: int = 30,
        from_memcache: bool = False,
        all_variants: bool = False,
    ):
        try:
            if trades_days > pairs_days:
                pairs_days = trades_days
            # Skip if cache not available yet
            if self.pairs_last_trade_cache is None:
                self.pairs_last_trade_cache = memcache.get_pairs_last_traded()
                msg = "skipping cache_calc.tickers, pairs_last_trade_cache is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            # Skip if cache not available yet
            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()
                msg = "skipping cache_calc.tickers, coins_config is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            suffix = transform.get_suffix(trades_days)
            ts = cron.now_utc() - pairs_days * 86400
            # Filter out pairs older than requested time
            pairs = sorted(
                [
                    i
                    for i in self.pairs_last_trade_cache
                    if self.pairs_last_trade_cache[i]["last_swap_time"] > ts
                ]
            )
            if from_memcache == 1:
                # Disabled for now
                # TODO: test if performance boost with this or not
                data = []
                key = "ticker_info"
                for i in pairs:
                    cache_name = derive.pair_cachename(key, i, suffix, all_variants)
                    cache_data = memcache.get(cache_name)
                    if cache_data is not None:
                        data.append(cache_data)
            else:
                data = [
                    Pair(
                        pair_str=i,
                        pairs_last_trade_cache=self.pairs_last_trade_cache,
                        coins_config=self.coins_config,
                    ).ticker_info(trades_days, all_variants=False)
                    for i in pairs
                ]

                data = [i for i in data if i is not None]
                data = clean.decimal_dict_lists(data, to_string=True, rounding=10)
                data = sortdata.dict_lists(data, "ticker_id")
                tickers = {}
                for i in data:
                    pair = deplatform.pair(i["ticker_id"])
                    variant = i["ticker_id"]
                    if pair not in tickers:
                        tickers.update({pair: {}})
                    tickers[pair].update(
                        {variant: convert.ticker_info_to_orderbook_extended(i)}
                    )

                resp = {
                    "pairs_count": len(data),
                    "combined_liquidity_usd": sumdata.json_key_10f(
                        data, "liquidity_in_usd"
                    ),
                    "tickers": tickers,
                }

                key = "orderbook_extended"
                cache_name = derive.pair_cachename(key, "", suffix)
                memcache.update(cache_name, resp, 900)

                msg = f"orderbook_extended complete! {len(pairs)} pairs traded"
                msg += f" in last {pairs_days} days"
            return default.result(resp, msg, loglevel="calc")
        except Exception as e:  # pragma: no cover
            msg = "orderbook_extended failed!"
            return default.error(e, msg)

    # MARKETS
    @timed
    def pairs_last_traded_markets(self):
        try:
            start_time = int(cron.days_ago(30))
            end_time = int(cron.now_utc())
            data = memcache.get_pairs_last_traded()
            pair_volumes = memcache.get_pair_volumes_24hr()
            coins_config = memcache.get_coins_config()
            filtered_data = {}
            for i in data:
                if data[i]["last_swap_time"] > start_time:
                    if data[i]["last_swap_time"] < end_time:
                        pair_std = i.replace("-segwit", "")
                        if pair_std not in filtered_data:
                            filtered_data.update({pair_std: {}})
                        filtered_data[pair_std].update(
                            {
                                i: convert.last_traded_to_market(
                                    i, data[i], pair_volumes, coins_config
                                )
                            }
                        )

            resp = [
                merge.segwit_pairs_last_traded_markets(filtered_data[i])
                for i in filtered_data
            ]
            logger.info(resp[0])
            resp = [clean.decimal_dicts(i) for i in resp]
            logger.calc(resp[0])
            msg = "pairs_last_traded_markets complete!"
            return default.result(resp, msg, loglevel="loop")
        except Exception as e:  # pragma: no cover
            msg = f"pairs_last_traded_markets failed! {e}"
            logger.warning(msg)

    # REVIEW
    @timed
    def tickers(
        self,
        trades_days: int = 1,
        pairs_days: int = 30,
        from_memcache: bool = False,
        all_variants: bool = False,
    ):
        try:
            if trades_days > pairs_days:
                pairs_days = trades_days
            # Skip if cache not available yet
            if self.pairs_last_trade_cache is None:
                self.pairs_last_trade_cache = memcache.get_pairs_last_traded()
                msg = "skipping cache_calc.tickers, pairs_last_trade_cache is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            # Skip if cache not available yet
            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()
                msg = "skipping cache_calc.tickers, coins_config is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            suffix = transform.get_suffix(trades_days)
            ts = cron.now_utc() - pairs_days * 86400
            # Filter out pairs older than requested time
            pairs = sorted(
                [
                    i
                    for i in self.pairs_last_trade_cache
                    if self.pairs_last_trade_cache[i]["last_swap_time"] > ts
                ]
            )
            if from_memcache == 1:
                # Disabled for now
                # TODO: test if performance boost with this or not
                data = []
                key = "ticker_info"
                for i in pairs:
                    cache_name = derive.pair_cachename(key, i, suffix, all_variants)
                    cache_data = memcache.get(cache_name)
                    if cache_data is not None:
                        data.append(cache_data)
            else:
                data = [
                    Pair(
                        pair_str=i,
                        pairs_last_trade_cache=self.pairs_last_trade_cache,
                        coins_config=self.coins_config,
                    ).ticker_info(trades_days, all_variants=False)
                    for i in pairs
                ]

                data = [i for i in data if i is not None]
                data = clean.decimal_dict_lists(data, to_string=True, rounding=10)
                data = sortdata.dict_lists(data, "ticker_id")

                resp = {
                    "pairs_count": len(data),
                    "combined_liquidity_usd": sumdata.json_key_10f(
                        data, "liquidity_in_usd"
                    ),
                    "tickers": data,
                }
                msg = f"Traded_tickers complete! {len(pairs)} pairs traded"
                msg += f" in last {pairs_days} days"
            return default.result(resp, msg, loglevel="calc")
        except Exception as e:  # pragma: no cover
            msg = "tickers failed!"
            return default.error(e, msg)
