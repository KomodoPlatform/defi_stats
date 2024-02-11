#!/usr/bin/env python3
from decimal import Decimal
from util.logger import logger, timed
from util.transform import clean, sortdata, sumdata, derive, convert, merge, template
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
        self.pairs_last_trade_cache = memcache.get_pair_last_traded()
        self.gecko_source = memcache.get_gecko_source()
        self.pg_query = db.SqlQuery()

    # FOUNDATIONAL CACHE
    @timed
    def pair_last_traded(self):
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
            resp = {}
            for variant in data:
                logger.calc(variant)
                depair = deplatform.pair(variant)
                if depair not in resp:
                    resp.update({depair: {"ALL": template.first_last_traded()}})
                resp[depair].update({variant: data[variant]})
                all = resp[depair]["ALL"]
                x = resp[depair][variant]
                all = merge.first_last_traded(all, x)
            msg = "pair_last_traded complete!"
            return default.result(resp, msg, loglevel="loop")
        except Exception as e:  # pragma: no cover
            msg = f"pair_last_traded failed! {e}"
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
    def pair_orderbook_extended(
        self, pairs_days: int = 30, from_memcache: bool = False
    ):
        try:
            cache_name = "pair_orderbook_extended"
            # Skip if cache not available yet
            if self.pairs_last_trade_cache is None:
                self.pairs_last_trade_cache = memcache.get_pair_last_traded()
                msg = "skipping cache_calc.tickers, pairs_last_trade_cache is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            # Skip if cache not available yet
            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()
                msg = "skipping cache_calc.tickers, coins_config is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            # Filter out pairs older than requested time
            ts = cron.now_utc() - pairs_days * 86400
            pairs = derive.pairs_traded_since(ts, self.pairs_last_trade_cache)
            cache_data = memcache.get(cache_name)
            if from_memcache and cache_data is not None:
                msg = f"Using cache for {cache_name}"
                return default.result(cache_data, msg, loglevel="cached")
            else:
                data = [
                    Pair(
                        pair_str=pair_str,
                        pairs_last_trade_cache=self.pairs_last_trade_cache,
                        coins_config=self.coins_config,
                    ).orderbook(pair_str, depth=100, no_thread=False)
                    for pair_str in pairs
                ]

                swaps = 0
                volume_in_usd = 0
                orderbook_data = {}
                liquidity_usd = 0
                for depair_data in data:
                    depair = deplatform.pair(depair_data["ALL"]["pair"])
                    if depair not in orderbook_data:
                        orderbook_data.update({depair: {}})
                    for variant in depair_data:
                        orderbook_data[depair].update({
                            variant: depair_data[variant]
                        })
                    swaps += int(depair_data["ALL"]["trades_24hr"])
                    volume_in_usd += Decimal(depair_data["ALL"]["volume_usd_24hr"])
                    liquidity_usd += Decimal(depair_data["ALL"]["liquidity_usd"])

                resp = clean.decimal_dicts({
                    "pairs_count": len(data),
                    "swaps_24hr": swaps,
                    "volume_usd_24hr": volume_in_usd,
                    "combined_liquidity_usd": liquidity_usd,
                    "orderbooks": orderbook_data,
                })
                if liquidity_usd > 0:
                    memcache.update(cache_name, resp, 300)

                msg = f"pair_orderbook_extended complete! {len(pairs)} pairs traded"
                msg += f" in last {pairs_days} days"
            return default.result(resp, msg, loglevel="calc")
        except Exception as e:  # pragma: no cover
            msg = "pair_orderbook_extended failed!"
            return default.error(e, msg)

    # MARKETS

    # REVIEW
    @timed
    def tickers(
        self, trades_days: int = 1, pairs_days: int = 30, from_memcache: bool = False
    ):
        try:
            return
            if trades_days > pairs_days:
                pairs_days = trades_days
            # Skip if cache not available yet
            if self.pairs_last_trade_cache is None:
                self.pairs_last_trade_cache = memcache.get_pair_last_traded()
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
            pairs = derive.pairs_traded_since(ts, self.pairs_last_trade_cache)
            if from_memcache == 1:
                # Disabled for now
                # TODO: test if performance boost with this or not
                data = []
                key = "ticker_info"
                for i in pairs:
                    cache_name = derive.pair_cachename(key, i, suffix)
                    cache_data = memcache.get(cache_name)
                    if cache_data is not None:
                        data.append(cache_data)
            else:
                for i in pairs:
                    p = Pair(
                        pair_str=i,
                        pairs_last_trade_cache=self.pairs_last_trade_cache,
                        coins_config=self.coins_config,
                    )

                data = {i: p.ticker_info(trades_days) for i in pairs}
                prices_data = {i: p.get_pair_prices_info(trades_days) for i in pairs}
                # logger.calc(prices_data.keys())
                for pair in data:
                    if pair in prices_data:
                        logger.loop(prices_data[pair].keys())
                        logger.merge(prices_data[pair]["prices"].keys())
                        logger.calc(f"data[pair].keys(): {data[pair].keys()}")
                        for variant in data[pair]:
                            if variant in prices_data[pair]["prices"]:
                                logger.calc(prices_data[pair]["prices"][variant])
                                # logger.calc(data[i][j])
                                # data[i][j].update(prices_data['prices'][i][j])
                                # logger.info(prices_data[pair]["prices"][variant])
                            else:
                                pass
                                # logger.warning(f"variant {variant} not found in prices_data!")
                    else:
                        logger.warning(f"{pair} not found in prices_data!")

                data_list = [i for i in data if i is not None]
                data = clean.decimal_dict_lists(data, to_string=True, rounding=10)
                data = sortdata.dict_lists(data, "ticker_id")

                pairs_count = len(data_list)
                combined_liquidity_usd = sumdata.json_key_10f(
                    data_list, "liquidity_usd"
                )

                tickers_data = {}
                for i in data:
                    pair = deplatform.pair(i["ticker_id"])
                    variant = i["ticker_id"]
                    if pair not in tickers_data:
                        tickers_data.update({pair: {}})
                    tickers_data[pair].update({variant: i})

                pairs_count = 0
                combined_liquidity_usd = 0
                resp = {
                    "pairs_count": pairs_count,
                    "combined_liquidity_usd": combined_liquidity_usd,
                    "tickers": tickers_data,
                }
                msg = f"Traded_tickers complete! {len(pairs)} pairs traded"
                msg += f" in last {pairs_days} days"
            return default.result(resp, msg, loglevel="calc")
        except Exception as e:  # pragma: no cover
            msg = "tickers failed!"
            return default.error(e, msg)

    @timed
    def pair_prices_24hr(self, days=1, from_memcache: bool = False):
        try:
            cache_name = "pair_prices_24hr"
            cache_data = memcache.get(cache_name)
            if from_memcache and cache_data is not None:
                msg = f"Using cache for {cache_name}"
                return default.result(cache_data, msg, loglevel="cached")
            else:
                if self.pairs_last_trade_cache is None:
                    self.pairs_last_trade_cache = memcache.get_pair_last_traded()
                    msg = "skipping cache_calc.tickers, pairs_last_trade_cache is None"
                    return default.result(msg=msg, loglevel="warning", data=None)
                
                # Skip if cache not available yet
                if self.coins_config is None:
                    self.coins_config = memcache.get_coins_config()
                    msg = "skipping cache_calc.tickers, coins_config is None"
                    return default.result(msg=msg, loglevel="warning", data=None)
                
                ts = cron.now_utc() - days * 86400
                # Filter out pairs older than requested time
                pairs = derive.pairs_traded_since(ts, self.pairs_last_trade_cache)
                prices_data = {
                    i: Pair(
                        pair_str=i,
                        pairs_last_trade_cache=self.pairs_last_trade_cache,
                        coins_config=self.coins_config,
                    ).get_pair_prices_info(days) for i in pairs
                }
                memcache.update(cache_name, prices_data, 600)
                msg = f"[{cache_name}] updated"
            return default.result(prices_data, msg, loglevel="calc")
        except Exception as e:  # pragma: no cover
            msg = f"prices failed! {e}"
            return default.error(e, msg)
           

    @timed
    def pairs_info_extended(
        self,
        trades_days: int = 1,
        pairs_days: int = 30,
        from_memcache: bool = False,
    ):
        try:
            return
            if trades_days > pairs_days:
                pairs_days = trades_days
            # Skip if cache not available yet
            if self.pairs_last_trade_cache is None:
                self.pairs_last_trade_cache = memcache.get_pair_last_traded()
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
            pairs = derive.pairs_traded_since(ts, self.pairs_last_trade_cache)
            if from_memcache == 1:
                # Disabled for now
                # TODO: test if performance boost with this or not
                data = []
                key = "pairs_info_extended"
                for i in pairs:
                    cache_name = derive.pair_cachename(key, i, suffix)
                    cache_data = memcache.get(cache_name)
                    if cache_data is not None:
                        data.append(cache_data)
            else:
                data = [
                    Pair(
                        pair_str=i,
                        pairs_last_trade_cache=self.pairs_last_trade_cache,
                        coins_config=self.coins_config,
                    ).ticker_info(trades_days)
                    for i in pairs
                ]

                data = [i for i in data if i is not None]
                data = clean.decimal_dict_lists(data, to_string=True, rounding=10)
                data = sortdata.dict_lists(data, "ticker_id")

                resp = {
                    "pairs_count": len(data),
                    "combined_liquidity_usd": sumdata.json_key_10f(
                        data, "liquidity_usd"
                    ),
                    "tickers": data,
                }
                msg = f"Traded_tickers complete! {len(pairs)} pairs traded"
                msg += f" in last {pairs_days} days"
            return default.result(resp, msg, loglevel="calc")
        except Exception as e:  # pragma: no cover
            msg = "tickers failed!"
            return default.error(e, msg)


    @timed
    def markets_summary(self):
        try:
            resp = []
            data = {}
            coins_config = memcache.get_coins_config()
            book = memcache.get_pair_orderbook_extended()
            vols = memcache.get_pair_volumes_24hr()
            last = memcache.get_pair_last_traded()
            prices = memcache.get_pair_prices_24hr()
            for depair in book["orderbooks"]:
                base, quote = derive.base_quote(depair)
                for variant in book["orderbooks"][depair]:
                    segwit_variants = derive.pair_variants(variant, segwit_only=True, coins_config=coins_config)
                    if variant == "ALL":
                        continue
                    else:
                        variant = variant.replace("-segwit", "")
                        existing = template.markets_summary(pair_str=variant)
                    if variant not in data:
                        for i in segwit_variants:
                            p = template.pair_prices_info(suffix="24hr", base=base, quote=quote)
                            o = book["orderbooks"][depair][i]
                            v = template.pair_trade_vol_item()
                            if depair in vols["volumes"]:
                                if i in vols["volumes"][depair]:
                                    v = vols["volumes"][depair][i]
                            if depair in prices:
                                if i in prices[depair]:
                                    p = prices[depair][i]
                            l = template.first_last_traded()
                            if depair in last:
                                if i in last[depair]:
                                    l = last[depair][i]
                            new = {
                                "liquidity_usd": v['base_volume'],
                                "base_volume": v['base_volume'],
                                "base_price_usd": p['base_price_usd'],
                                "quote_volume": v['quote_volume'],
                                "quote_price_usd": p['quote_price_usd'],
                                "lowest_ask": o["lowest_ask"],
                                "highest_bid": o["highest_bid"],
                                "lowest_price_24hr": o['lowest_price_24hr'],
                                "highest_price_24hr": o['highest_price_24hr'],
                                "price_change_24hr": o['price_change_24hr'],
                                "price_change_pct_24hr": o['price_change_pct_24hr'],
                                "last_price": l['last_swap_price'],
                                "newest_price": o['newest_price'],
                                "newest_price_time": o['newest_price_time'],
                                "oldest_price": o['oldest_price'],
                                "oldest_price_time": o['oldest_price_time'],
                                "last_swap": l['last_swap_time'],
                                "last_swap_uuid": l['last_swap_uuid'],
                                "trades_24hr": o["trades_24hr"],
                                "liquidity_usd": o["liquidity_usd"],
                                "volume_usd_24hr": o["volume_usd_24hr"],
                                "variants": segwit_variants
                            }
                            merged = merge.market_summary(existing, new)
                            existing = clean.decimal_dicts(merged)
                            if depair == "KMD_LTC":
                                pass
                                '''
                                logger.pair(f"existing: {existing}")
                                logger.calc(f"new: {new}")
                                logger.merge(f"merged: {merged}")
                                logger.query(f"existing: {existing}")
                                '''
                            data.update({variant: existing})
                resp = [i for i in data.values()]
            return resp
        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [/api/v3/market/summary]: {e}")
            return {"error": f"{type(e)} Error in [/api/v3/market/summary]: {e}"}
           

