#!/usr/bin/env python3
from decimal import Decimal
from util.logger import logger, timed
from util.transform import clean, derive, merge, template, convert
import db.sqldb as db
from util.cron import cron
import util.defaults as default
import util.helper as helper
import util.memcache as memcache
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
                depair = deplatform.pair(variant)
                if depair not in resp:
                    resp.update({depair: {"ALL": template.first_last_traded()}})
                resp[depair].update({variant: data[variant]})
                all = resp[depair]["ALL"]
                x = resp[depair][variant]
                all = merge.first_last_traded(all, x)
            msg = "pair_last_traded complete!"
            return default.result(resp, msg, loglevel="loop", ignore_until=3)
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
            return default.result(vols_usd, msg, loglevel="loop", ignore_until=3)
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
            return default.result(vols_usd, msg, loglevel="loop", ignore_until=3)
        except Exception as e:  # pragma: no cover
            msg = f"pair_volumes_24hr failed! {e}"
            logger.warning(msg)

    @timed
    def pair_orderbook_extended(self, pairs_days: int = 30, no_thread=True):
        try:
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

            data = [
                Pair(
                    pair_str=pair_str,
                    pairs_last_trade_cache=self.pairs_last_trade_cache,
                    coins_config=self.coins_config,
                ).orderbook(pair_str, depth=100, no_thread=no_thread)
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
                    orderbook_data[depair].update(
                        {variant: clean.decimal_dicts(depair_data[variant])}
                    )
                swaps += int(depair_data["ALL"]["trades_24hr"])
                volume_in_usd += Decimal(depair_data["ALL"]["volume_usd_24hr"])
                liquidity_usd += Decimal(depair_data["ALL"]["liquidity_usd"])

            resp = clean.decimal_dicts(
                {
                    "pairs_count": len(data),
                    "swaps_24hr": swaps,
                    "volume_usd_24hr": volume_in_usd,
                    "combined_liquidity_usd": liquidity_usd,
                    "orderbooks": orderbook_data,
                }
            )

            msg = f"pair_orderbook_extended complete! {len(pairs)} pairs traded"
            msg += f" in last {pairs_days} days"
            return default.result(resp, msg, loglevel="calc", ignore_until=3)
        except Exception as e:  # pragma: no cover
            msg = "pair_orderbook_extended failed!"
            return default.error(e, msg)

    @timed
    def tickers(self, refresh: bool = False):
        try:
            resp = memcache.get_tickers()
            if resp is None or refresh:
                coins = memcache.get_coins_config()
                book = memcache.get_pair_orderbook_extended()
                volumes = memcache.get_pair_volumes_24hr()
                prices = memcache.get_pair_volumes_24hr()
                resp = {
                    "last_update": int(cron.now_utc()),
                    "pairs_count": book["pairs_count"],
                    "swaps_count": book["swaps_24hr"],
                    "combined_volume_usd": book["volume_usd_24hr"],
                    "combined_liquidity_usd": book["combined_liquidity_usd"],
                    "data": {},
                }
                for depair in book["orderbooks"]:
                    b = book["orderbooks"][depair]["ALL"]
                    if depair in volumes["volumes"]:
                        v = volumes["volumes"][depair]["ALL"]
                    else:
                        v = template.pair_trade_vol_item()
                    if depair in prices:
                        p = prices[depair]["ALL"]
                    else:
                        p = template.pair_prices_info(suffix="24hr")
                    resp["data"].update(
                        {
                            depair: convert.pair_orderbook_extras_to_gecko_tickers(
                                b, v, p, coins
                            )
                        }
                    )
                    memcache.set_tickers(resp)
            return resp
        except Exception as e:  # pragma: no cover
            msg = "tickers failed!"
            return default.error(e, msg)

    @timed
    def pair_prices_24hr(self, days=1, from_memcache: bool = False):
        try:
            if self.pairs_last_trade_cache is None:
                self.pairs_last_trade_cache = memcache.get_pair_last_traded()
                msg = "skipping cache_calc.tickers, pairs_last_trade_cache is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            # Skip if cache not available yet
            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()
                msg = "skipping cache_calc.tickers, coins_config is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            pair_vols = memcache.get_pair_volumes_24hr()

            ts = cron.now_utc() - days * 86400
            # Filter out pairs older than requested time
            pairs = derive.pairs_traded_since(ts, self.pairs_last_trade_cache)
            prices_data = {
                i: Pair(
                    pair_str=i,
                    pairs_last_trade_cache=self.pairs_last_trade_cache,
                    coins_config=self.coins_config,
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
                            {"swaps": 0, "trade_volume_usd": 0}
                        )
                        if depair in pair_vols["volumes"]:
                            if variant in pair_vols["volumes"][depair]:
                                resp[depair][variant].update(
                                    {
                                        "swaps": pair_vols["volumes"][depair][variant][
                                            "swaps"
                                        ],
                                        "trade_volume_usd": pair_vols["volumes"][
                                            depair
                                        ][variant]["trade_volume_usd"],
                                    }
                                )

            msg = "[pair_prices_24hr] update loop complete"
            return default.result(resp, msg, loglevel="calc", ignore_until=3)
        except Exception as e:  # pragma: no cover
            msg = f"prices failed! {e}"
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
                    segwit_variants = derive.pair_variants(
                        variant, segwit_only=True, coins_config=coins_config
                    )
                    if variant == "ALL":
                        continue
                    else:
                        variant = variant.replace("-segwit", "")
                        existing = template.markets_summary(pair_str=variant)
                    if variant not in data:
                        for i in segwit_variants:
                            p = template.pair_prices_info(suffix="24hr")
                            o = book["orderbooks"][depair][i]
                            v = template.pair_trade_vol_item()
                            if depair in vols["volumes"]:
                                if i in vols["volumes"][depair]:
                                    v = vols["volumes"][depair][i]
                            if depair in prices:
                                if i in prices[depair]:
                                    p = prices[depair][i]
                            lt = template.first_last_traded()
                            if depair in last:
                                if i in last[depair]:
                                    lt = last[depair][i]
                            new = {
                                "base_volume": v["base_volume"],
                                "quote_volume": v["quote_volume"],
                                "lowest_ask": o["lowest_ask"],
                                "highest_bid": o["highest_bid"],
                                "lowest_price_24hr": o["lowest_price_24hr"],
                                "highest_price_24hr": o["highest_price_24hr"],
                                "price_change_24hr": o["price_change_24hr"],
                                "price_change_pct_24hr": o["price_change_pct_24hr"],
                                "last_price": lt["last_swap_price"],
                                "newest_price": o["newest_price"],
                                "newest_price_time": o["newest_price_time"],
                                "oldest_price": o["oldest_price"],
                                "oldest_price_time": o["oldest_price_time"],
                                "last_swap": lt["last_swap_time"],
                                "last_swap_uuid": lt["last_swap_uuid"],
                                "trades_24hr": o["trades_24hr"],
                                "liquidity_usd": o["liquidity_usd"],
                                "volume_usd_24hr": o["volume_usd_24hr"],
                                "base_price_usd": p["base_price_usd"],
                                "quote_price_usd": p["quote_price_usd"],
                                "variants": segwit_variants,
                            }
                            merged = merge.market_summary(existing, new)
                            existing = clean.decimal_dicts(merged)
                        # remove where no past trades detected
                        if lt["last_swap_uuid"] != "":
                            data.update({variant: existing})
                resp = [i for i in data.values()]
            return resp
        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [/api/v3/market/summary]: {e}")
            return {"error": f"{type(e)} Error in [/api/v3/market/summary]: {e}"}

    @timed
    def stats_api_summary(self, refresh: bool = False):
        try:
            resp = memcache.get_stats_api_summary()
            if resp is None or refresh:
                resp = []
                coins = memcache.get_coins()
                book = memcache.get_pair_orderbook_extended()
                vols = memcache.get_pair_volumes_24hr()
                last = memcache.get_pair_last_traded()
                prices = memcache.get_pair_prices_24hr()
                for depair in book["orderbooks"]:
                    o = book["orderbooks"][depair]["ALL"]
                    lt = template.first_last_traded()
                    p = template.pair_prices_info(suffix="24hr")
                    v = template.pair_trade_vol_item()

                    if depair in vols["volumes"]:
                        if "ALL" in vols["volumes"][depair]:
                            v = vols["volumes"][depair]["ALL"]
                    if depair in prices:
                        if "ALL" in prices[depair]:
                            p = prices[depair]["ALL"]
                    if depair in last:
                        if "ALL" in last[depair]:
                            lt = last[depair]["ALL"]
                    variants = derive.pair_variants(pair_str=depair, coins_config=coins)
                    data = clean.decimal_dicts({
                        "ticker_id": depair,
                        "base_currency": o["base"],
                        "base_trade_value_usd": v["base_volume_usd"],
                        "base_liquidity_coins": o["base_liquidity_coins"],
                        "base_liquidity_usd": o["base_liquidity_usd"],
                        "base_volume": v["base_volume"],
                        "quote_currency": o["quote"],
                        "quote_trade_value_usd": v["quote_volume_usd"],
                        "quote_liquidity_coins": o["quote_liquidity_coins"],
                        "quote_liquidity_usd": o["quote_liquidity_usd"],
                        "quote_volume": v["quote_volume"],
                        "lowest_ask": o["lowest_ask"],
                        "highest_bid": o["highest_bid"],
                        "lowest_price_24h": o["lowest_price_24hr"],
                        "highest_price_24h": o["highest_price_24hr"],
                        "price_change_24h": o["price_change_24hr"],
                        "price_change_percent_24h": o["price_change_pct_24hr"],
                        "newest_price": o["newest_price"],
                        "newest_price_time": o["newest_price_time"],
                        "oldest_price": o["oldest_price"],
                        "oldest_price_time": o["oldest_price_time"],
                        "last_price": lt["last_swap_price"],
                        "last_trade": lt["last_swap_time"],
                        "last_swap_uuid": lt["last_swap_uuid"],
                        "pair_swaps_count": o["trades_24hr"],
                        "pair_liquidity_usd": o["liquidity_usd"],
                        "volume_usd_24h": o["volume_usd_24hr"],
                        "pair_trade_value_usd": o["volume_usd_24hr"],
                        "base_price_usd": p["base_price_usd"],
                        "quote_price_usd": p["quote_price_usd"],
                        "variants": variants,
                    })
                    # remove where no past trades detected
                    if lt["last_swap_uuid"] != "":
                        resp.append(data)
            return resp
        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [/api/v3/stats_api/summary]: {e}")
            return {"error": f"{type(e)} Error in [/api/v3/stats_api/summary]: {e}"}

    @timed
    def gecko_pairs(self, refresh: bool = False):
        resp = memcache.get_gecko_pairs()
        if resp is None or refresh:
            coins = memcache.get_coins_config()
            cache = memcache.get_pair_last_traded()
            ts = cron.now_utc() - 86400 * 7
            pairs = derive.pairs_traded_since(ts=ts, pairs_last_trade_cache=cache)
            resp = [template.gecko_pair_item(i, coins) for i in pairs]
        return resp
