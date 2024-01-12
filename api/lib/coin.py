#!/usr/bin/env python3
from decimal import Decimal
from lib.cache import (
    load_coins_config,
    load_gecko_source,
    load_generic_last_traded,
)
from util.defaults import set_params
from util.logger import logger


class Coin:
    def __init__(self, coin: str = "KMD", **kwargs):
        try:
            # Set params
            self.kwargs = kwargs
            self.options = ["testing"]
            set_params(self, self.kwargs, self.options)
            self.coin = coin
            self.ticker = self.coin.split("-")[0]
            if "gecko_source" in kwargs:
                self.gecko_source = kwargs["gecko_source"]
            else:
                self.gecko_source = load_gecko_source()
            if "coins_config" in kwargs:
                self.coins_config = kwargs["coins_config"]
            else:
                self.coins_config = load_coins_config()

            if "last_traded_cache" in kwargs:
                self.last_traded_cache = kwargs["last_traded_cache"]
            else:
                self.last_traded_cache = load_generic_last_traded()

            # Designate coin
            if self.coin in self.coins_config:
                self.type = self.coins_config[self.coin]["type"]
                self.is_testnet = self.coins_config[self.coin]["is_testnet"]
                self.is_wallet_only = self.coins_config[self.coin]["wallet_only"]
            else:
                self.type = "Delisted"
                self.is_testnet = False
                self.is_wallet_only = True
        except Exception as e:  # pragma: no cover
            msg = f"Init Coin for {coin} failed!"
            logger.error(f"{type(e)} {msg}: {e}")

    @property
    def usd_price(self):
        return get_gecko_price(ticker=self.coin, gecko_source=self.gecko_source)

    @property
    def mcap(self):
        return get_gecko_mcap(ticker=self.coin, gecko_source=self.gecko_source)

    @property
    def is_priced(self):
        return self.usd_price > 0

    @property
    def is_tradable(self):
        if self.coin in self.coins_config:
            if self.is_wallet_only:
                return False
            return True
        return False

    @property
    def has_segwit(self):
        if self.coin.endswith("-segwit"):
            return True
        if f"{self.coin}-segwit" in self.coins_config:
            return True
        return False

    @property
    def is_valid(self):
        if self.is_tradable:
            return True
        if self.has_segwit:  # pragma: no cover
            for i in [self.ticker, f"{self.ticker}-segwit"]:
                if i in self.coins_config:
                    if not self.coins_config[i]["wallet_only"]:
                        return True
        return False

    @property
    def related_coins(self):
        data = [
            Coin(coin=i, gecko_source=self.gecko_source)
            for i in self.coins_config.keys()
            if i == self.coin or i.startswith(f"{self.ticker}-")
        ]
        return data


def get_gecko_price(ticker, gecko_source=None, testing=False) -> float:
    try:
        if gecko_source is None:
            gecko_source = load_gecko_source()
        if ticker in gecko_source:
            return Decimal(gecko_source[ticker]["usd_price"])
    except KeyError as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: [KeyError] {e}")
    except Exception as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: {e}")
    return Decimal(0)  # pragma: no cover


def get_gecko_mcap(ticker, gecko_source=None, testing=False) -> float:
    try:
        if gecko_source is None:
            gecko_source = load_gecko_source()
        if ticker in gecko_source:
            return Decimal(gecko_source[ticker]["usd_market_cap"])
    except KeyError as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: [KeyError] {e}")
    except Exception as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: {e}")
    return Decimal(0)  # pragma: no cover
