#!/usr/bin/env python3
from dataclasses import dataclass
from decimal import Decimal
import lib
from util.logger import logger
import util.defaults as default
import util.helper as helper
import util.memcache as memcache
import util.templates as template
import util.transform as transform


@dataclass
class Coins:  # pragma: no cover
    def __init__(self):
        coins_config = memcache.get_coins_config()
        self.coins = [lib.Coin(coin=i) for i in coins_config]

    @property
    def with_price(self):
        return [i for i in self.coins if i.is_priced]

    @property
    def with_mcap(self):
        return [i for i in self.coins if i.mcap > 0]

    @property
    def with_segwit(self):
        data = [i for i in self.coins if i.has_segwit]
        return data

    @property
    def tradable_only(self):
        return [i for i in self.coins if i.is_tradable]

    @property
    def valid_only(self):
        return [i for i in self.coins if i.is_valid]

    @property
    def wallet_only(self):
        return [i for i in self.coins if i.is_wallet_only]

    @property
    def testnet_only(self):
        return [i for i in self.coins if i.is_testnet]

    @property
    def by_type(self):
        for coin in self.coins:
            if coin.type not in self.by_type_data:
                self.by_type_data.update({coin.type: []})
            self.by_type_data[coin.type].append(coin)
        return self.by_type_data


class Coin:
    def __init__(self, coin: str = "KMD", **kwargs):
        try:
            # Set params
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.coin = coin
            self.ticker = self.coin.split("-")[0]
            coins_config = memcache.get_coins_config()

            # Designate coin
            if self.coin in coins_config:
                self.type = coins_config[self.coin]["type"]
                self.is_testnet = coins_config[self.coin]["is_testnet"]
                self.is_wallet_only = coins_config[self.coin]["wallet_only"]
            else:
                self.type = "Delisted"
                self.is_testnet = False
                self.is_wallet_only = True
        except Exception as e:  # pragma: no cover
            msg = f"Init Coin for {coin} failed!"
            logger.error(f"{type(e)} {msg}: {e}")

    @property
    def usd_price(self):
        return get_gecko_price(ticker=self.coin)

    @property
    def mcap(self):
        return get_gecko_mcap(ticker=self.coin)

    @property
    def is_priced(self):
        return self.usd_price > 0

    @property
    def is_tradable(self):
        coins_config = memcache.get_coins_config()
        if self.coin in coins_config:
            if self.is_wallet_only:
                return False
            return True
        return False

    @property
    def has_segwit(self):
        coins_config = memcache.get_coins_config()
        if self.coin.endswith("-segwit"):
            return True
        if f"{self.coin}-segwit" in coins_config:
            return True
        return False

    @property
    def is_valid(self):
        coins_config = memcache.get_coins_config()
        if self.is_tradable:
            return True
        if self.has_segwit:  # pragma: no cover
            for i in [self.ticker, f"{self.ticker}-segwit"]:
                if i in coins_config:
                    if not coins_config[i]["wallet_only"]:
                        return True
        return False


def get_gecko_price(ticker) -> float:
    try:
        gecko_source = memcache.get_gecko_source()
        if ticker in gecko_source:
            return Decimal(gecko_source[ticker]["usd_price"])
    except KeyError as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: [KeyError] {e}")
    except Exception as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: {e}")
    return Decimal(0)  # pragma: no cover


def get_gecko_mcap(ticker) -> float:
    try:
        gecko_source = memcache.get_gecko_source()
        if ticker in gecko_source:
            return Decimal(gecko_source[ticker]["usd_market_cap"])
    except KeyError as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: [KeyError] {e}")
    except Exception as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: {e}")
    return Decimal(0)  # pragma: no cover


def pair_without_segwit_suffix(maker_coin, taker_coin):
    """Removes `-segwit` suffixes from the tickers of a pair"""
    if maker_coin.endswith("-"):
        maker_coin = maker_coin[:-1]
    if taker_coin.endswith("-"):
        taker_coin = taker_coin[:-1]
    return f'{maker_coin.replace("-segwit", "")}_{taker_coin.replace("-segwit", "")}'


def get_coin_variants(coin, segwit_only=False):
    coins_config = memcache.get_coins_config()
    coin = coin.split("-")[0]
    data = [
        i
        for i in coins_config
        if (i.replace(coin, "") == "" or i.replace(coin, "").startswith("-"))
        and (not segwit_only or i.endswith("segwit") or i.replace(coin, "") == "")
    ]
    return data


def get_pair_variants(pair, segwit_only=False):
    base, quote = helper.base_quote_from_pair(pair)
    base_variants = get_coin_variants(base, segwit_only=segwit_only)
    quote_variants = get_coin_variants(quote, segwit_only=segwit_only)
    variants = []
    for i in base_variants:
        for j in quote_variants:
            if i != j:
                variants.append(f"{i}_{j}")
    return variants


def get_pairs_info(pair_list: str, priced: bool = False) -> list:
    try:
        return [template.pair_info(i, priced) for i in pair_list]
    except Exception as e:  # pragma: no cover
        return default.error(e)


def get_pair_info_sorted(pair_list: str, priced: bool = False) -> dict:
    try:
        return sorted(
            get_pairs_info(pair_list, priced),
            key=lambda d: d["ticker_id"],
        )
    except Exception as e:  # pragma: no cover
        return default.error(e)


def get_segwit_coins():
    data = memcache.get('coins_with_segwit')
    if data is None:
        coins = Coins()
        data = [i.coin for i in coins.with_segwit]
        memcache.update('coins_with_segwit', data, 86400)
    return data


def get_tradable_coins():
    coins = Coins()
    return coins.tradable_only


def get_kmd_pairs():
    coins = Coins()
    coins_with_price = [i.coin for i in coins.with_price]
    kmd_pairs = lib.get_all_coin_pairs("KMD", coins_with_price)
    kmd_pairs.sort()
    return kmd_pairs
