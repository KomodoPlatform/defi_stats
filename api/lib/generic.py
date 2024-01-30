#!/usr/bin/env python3
from decimal import Decimal
import db
import lib
from lib.coins import get_gecko_price, get_kmd_pairs
from util.exceptions import DataStructureError
from util.logger import timed, logger
from util.transform import sortdata, clean, merge
import util.cron as cron
import util.defaults as default
import util.helper as helper
import util.memcache as memcache
import util.templates as template
import util.transform as transform
import lib.dex_api as dex


class Generic:  # pragma: no cover
    def __init__(self, **kwargs) -> None:
        try:
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.pg_query = db.SqlQuery()
            self.last_traded_cache = memcache.get_last_traded()
            self.coins_config = memcache.get_coins_config()
            self.gecko_source = memcache.get_gecko_source()

        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Generic: {e}")

    @timed
    def pairs(self, days: int = 90) -> dict:
        """Returns basic pair info and tags as priced/unpriced"""
        try:
            pairs = self.pg_query.get_pairs(days=days)
            if "error" in pairs:  # pragma: no cover
                raise DataStructureError(
                    f"'get_pairs' returned an error: {pairs['error']}"
                )
            else:
                resp = get_pairs_status(pairs)
                resp = clean.decimal_dict_list(resp)
                self.last_traded_cache = memcache.get_last_traded()
                for i in resp:
                    first_last_swap = template.first_last_swap()
                    if self.last_traded_cache is not None:
                        if i["ticker_id"] in self.last_traded_cache:
                            x = self.last_traded_cache[i["ticker_id"]]
                            first_last_swap = clean.decimal_dict(x)
                    i.update(first_last_swap)
                msg = f"{len(pairs)} pairs traded in the last {days} days"
                return default.result(data=resp, msg=msg, loglevel="loop")
        except Exception as e:  # pragma: no cover
            msg = f"Generic.pairs failed! {e}"
            return default.result(
                data=resp, msg=msg, loglevel="warning", ignore_until=2
            )

    @timed
    def orderbook(
        self,
        pair_str: str = "KMD_LTC",
        depth: int = 100,
        all: bool = False,
    ):
        try:
            if all:
                pair_str = transform.strip_pair_platforms(pair_str)
                pair_tpl = helper.base_quote_from_pair(pair_str)
                cache_name = f"orderbook_{pair_str}_ALL"
                variants = helper.get_pair_variants(pair_str)
                # logger.loop(f"{pair_str}: {variants}")
            else:
                # This will be a single ticker_pair unless for segwit
                pair_tpl = helper.base_quote_from_pair(pair_str)
                cache_name = f"orderbook_{pair_str}"
                variants = helper.get_pair_variants(pair_str, segwit_only=True)
                # logger.calc(f"{pair_str}: {variants}")
            if len(pair_tpl) != 2 or "error" in pair_tpl:
                return {"error": "Market pair should be in `KMD_BTC` format"}
            combined_orderbook = template.orderbook(pair_str)
            combined_orderbook.update({"variants": variants})

            # Use combined cache if valid
            orderbook_data = memcache.get(cache_name)
            if (
                orderbook_data is not None
                and len(orderbook_data["asks"]) > 0
                and len(orderbook_data["bids"]) > 0
            ):
                # logger.calc(f"{cache_name} exists and is populated!")
                combined_orderbook = orderbook_data
            else:
                if self.coins_config is None:
                    self.coins_config = memcache.get_coins_config()

                if self.gecko_source is None:
                    self.gecko_source = memcache.get_gecko_source()

                for variant in variants:
                    variant_cache_name = f"orderbook_{variant}"
                    # Use variant cache if valid
                    data = memcache.get(variant_cache_name)
                    if data is not None and (
                        len(data["asks"]) > 0 or len(data["bids"]) > 0
                    ):
                        pass
                        # logger.calc(f"variant {variant_cache_name} exists, and is populated!")
                    else:
                        base, quote = helper.base_quote_from_pair(variant)
                        # Avoid duplication for utxo coins with segwit
                        # TODO: cover where legacy is wallet only
                        if base.endswith("-segwit") and len(variants) > 1:
                            continue
                        if quote.endswith("-segwit") and len(variants) > 1:
                            continue
                        data = dex.get_orderbook(
                            base=base,
                            quote=quote,
                            coins_config=self.coins_config,
                            gecko_source=self.gecko_source,
                            variant_cache_name=variant_cache_name,
                            depth=depth,
                        )
                    # Apply depth limit after caching so the cache is complete.
                    data["bids"] = data["bids"][:depth][::-1]
                    data["asks"] = data["asks"][::-1][:depth]
                    # TODO: Recalc liquidity if depth is less than data.
                    # Merge with other variants
                    combined_orderbook = merge.orderbooks(combined_orderbook, data)
                # Sort variant bids / asks
                combined_orderbook["bids"] = combined_orderbook["bids"][: int(depth)][
                    ::-1
                ]
                combined_orderbook["asks"] = combined_orderbook["asks"][::-1][
                    : int(depth)
                ]
                combined_orderbook = clean.orderbook_data(combined_orderbook)
                combined_orderbook["pair"] = pair_str
                combined_orderbook["base"] = pair_tpl[0]
                combined_orderbook["quote"] = pair_tpl[1]
                # logger.info(combined_orderbook['liquidity_in_usd'])

                # update the combined cache
                memcache.update(cache_name, combined_orderbook, 240)

            msg = f"Generic.orderbook for {pair_str} ({len(variants)} variants) complete with ${combined_orderbook['liquidity_in_usd']} liquidity"

            return default.result(
                data=combined_orderbook, msg=msg, loglevel="pair", ignore_until=2
            )
        except Exception as e:  # pragma: no cover
            msg = f"Generic.orderbook {pair_str} failed: {e}!"
            try:
                data = template.orderbook(pair_str)
                msg += f" Returning template!"
            except Exception as e:
                data = {"error": msg}
                return default.result(
                    data=data, msg=msg, loglevel="warning", ignore_until=0
                )

    @timed
    def tickers(self, trades_days: int = 1, pairs_days: int = 7):
        try:
            # Skip if cache not available yet
            if self.last_traded_cache is None:
                self.last_traded_cache = memcache.get_last_traded()
                msg = "skipping generic.tickers, last_traded_cache is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            # Skip if cache not available yet
            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()
                msg = "skipping generic.tickers, coins_config is None"
                return default.result(msg=msg, loglevel="warning", data=None)

            suffix = transform.get_suffix(trades_days)
            ts = cron.now_utc() - pairs_days * 86400

            # Filter out pairs older than requested time
            pairs = sorted(
                [
                    i
                    for i in self.last_traded_cache
                    if self.last_traded_cache[i]["last_swap_time"] > ts
                ]
            )
            # logger.info(f"{len(pairs)} pairs in last 90 days")

            data = [
                lib.Pair(
                    pair_str=i,
                    last_traded_cache=self.last_traded_cache,
                    coins_config=self.coins_config,
                ).ticker_info(trades_days, all=False)
                for i in pairs
            ]

            data = [i for i in data if i is not None]
            data = clean.decimal_dict_list(data, to_string=True, rounding=10)
            data = sortdata.sort_dict_list(data, "ticker_id")
            data = {
                "last_update": int(cron.now_utc()),
                "pairs_count": len(data),
                "swaps_count": int(transform.sum_json_key(data, f"trades_{suffix}")),
                "combined_volume_usd": transform.sum_json_key_10f(
                    data, "combined_volume_usd"
                ),
                "combined_liquidity_usd": transform.sum_json_key_10f(
                    data, "liquidity_in_usd"
                ),
                "data": data,
            }
            msg = f"Traded_tickers complete! {len(pairs)} pairs traded"
            msg += f" in last {pairs_days} days"
            return default.result(data, msg, loglevel="calc")
        except Exception as e:  # pragma: no cover
            msg = "tickers failed!"
            return default.error(e, msg)

    @timed
    def last_traded(self):
        try:
            if self.gecko_source is None:
                self.gecko_source = memcache.get_gecko_source()
            data = self.pg_query.pair_last_trade()
            price_status_dict = get_price_status_dict(data.keys(), self.gecko_source)
            for i in data:
                data[i] = clean.decimal_dict(data[i])
                data[i].update({"priced": get_pair_priced_status(i,  price_status_dict)})
                
            return data
        except Exception as e:  # pragma: no cover
            msg = "pairs_last_traded failed!"
            logger.warning(msg)


def get_pair_priced_status(pair,  price_status_dict):
    if pair in price_status_dict["priced_gecko"]:
        return True
    else:
        return False
    

def get_price_status_dict(pairs, gecko_source=None):
    try:
        if gecko_source is None:
            gecko_source = memcache.get_gecko_source()
        pairs_dict = {"priced_gecko": [], "unpriced": []}
        for pair_str in pairs:
            base, quote = helper.base_quote_from_pair(pair_str)
            base_price = get_gecko_price(base, gecko_source=gecko_source)
            quote_price = get_gecko_price(quote, gecko_source=gecko_source)
            if base_price > 0 and quote_price > 0:
                pairs_dict["priced_gecko"].append(pair_str)
            else:  # pragma: no cover
                pairs_dict["unpriced"].append(pair_str)
        return pairs_dict
    except Exception as e:  # pragma: no cover
        msg = "pairs_last_traded failed!"
        return default.error(e, msg)


@timed
def get_pairs_status(pairs, gecko_source=None):
    if gecko_source is None:
        gecko_source = memcache.get_gecko_source()
    pairs = list(set(pairs + get_kmd_pairs()))
    pairs_dict = get_price_status_dict(pairs, gecko_source)
    priced_pairs = helper.get_pairs_info(pairs_dict["priced_gecko"], True)
    unpriced_pairs = helper.get_pairs_info(pairs_dict["unpriced"], False)
    return sortdata.sort_dict_list(priced_pairs + unpriced_pairs, "ticker_id")
