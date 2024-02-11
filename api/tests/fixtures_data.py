#!/usr/bin/env python3
from decimal import Decimal

coins_config = {
    "NOSWAP": {"wallet_only": True, "is_testnet": False},
    "TEST": {"wallet_only": False, "is_testnet": True},
    "OK": {"wallet_only": False, "is_testnet": False},
}

swap_item = {
    "pair": "KMD_LTC-segwit",
    "uuid": "7d36be55-6db3-4662-93af-74dc73a58bfa",
    "trade_type": "buy",
    "started_at": "1700000776",
    "finished_at": "1700000777",
    "is_success": "1",
    "maker_coin": "KMD",
    "maker_coin_ticker": "KMD",
    "maker_coin_platform": "",
    "maker_amount": "100",
    "maker_coin_usd_price": "0.5",
    "taker_coin": "LTC-segwit",
    "taker_coin_ticker": "LTC",
    "taker_coin_platform": "segwit",
    "taker_amount": "1",
    "taker_coin_usd_price": "50.0",
    "price": 0.01,
    "reverse_price": 100,
}

swap_item2 = {
    "pair": "KMD_LTC-segwit",
    "uuid": "7d36be55-6db3-4662-93af-74dc73a58bfa",
    "trade_type": "sell",
    "started_at": "1700000776",
    "finished_at": "1700000000",
    "is_success": "0",
    "maker_coin": "LTC-segwit",
    "maker_coin_ticker": "LTC",
    "maker_coin_platform": "segwit",
    "maker_amount": "1",
    "maker_coin_usd_price": "50.0",
    "taker_coin": "KMD",
    "taker_coin_ticker": "KMD",
    "taker_coin_platform": "",
    "taker_amount": "100",
    "taker_coin_usd_price": "0.5",
    "price": 0.01,
    "reverse_price": 100,
}

cipi_swap = {
    "started_at": "2024-01-31 16:39:59",
    "taker_coin": "KMD",
    "uuid": "ugyfijoks",
    "taker_amount": "100",
    "taker_gui": "Komodo Wallet 0.9.0 Android; BT=1706085862",
    "taker_version": "2.0.0-beta_b0fd99e",
    "maker_coin": "LTC",
    "maker_amount": "1",
    "maker_gui": "Komodo Wallet 0.7.0-beta",
    "maker_version": "2.0.0-beta_b0fd99e",
    "status": "success",
}

cipi_swap2 = {
    "started_at": "2024-01-31 16:39:59",
    "maker_coin": "KMD",
    "uuid": "djhfgtkkj",
    "maker_amount": "100",
    "maker_gui": "Komodo Wallet 0.9.0 Android; BT=1706085862",
    "maker_version": "2.0.0-beta_b0fd99e",
    "taker_coin": "LTC",
    "taker_amount": "1",
    "taker_gui": "Komodo Wallet 0.7.0-beta",
    "taker_version": "2.0.0-beta_b0fd99e",
    "status": "success",
}

valid_tickers = ["KMD_LTC", "KMD_DASH", "KMD_BTC"]


class SampleData:
    def __init__(self) -> None:
        pass

    def ticker_item(self, suffix="24hr"):
        return {
            "ticker_id": "DGB_LTC",
            "pool_id": "DGB_LTC",
            "variants": [
                "DGB_LTC",
                "DGB-segwit_LTC",
                "DGB_LTC-segwit",
                "DGB-segwit_LTC-segwit",
            ],
            "base_currency": "DGB",
            "base_price_usd": "0.0100000000",
            "quote_currency": "LTC",
            "quote_price_usd": "100.0000000000",
            "last_swap_price": "1.0000000000",
            "last_swap_time": "1704858849",
            "last_swap_uuid": "55555555-ee4b-494f-a2fb-48467614b613",
            "oldest_price": "1.0000000000",
            "newest_price": "1.0000000000",
            "oldest_price_time": 1704969700,
            "newest_price_time": 1704969700,
            "highest_bid": "0.0001184354",
            "lowest_ask": "0.0001217309",
            f"highest_price_{suffix}": "1.0000000000",
            f"lowest_price_{suffix}": "1.0000000000",
            "liquidity_usd": "2290.2992432883",
            "base_liquidity_coins": "145136.4015657607",
            "base_liquidity_usd": "1451.3640156576",
            "quote_liquidity_coins": "8.3893522763",
            "quote_liquidity_usd": "838.9352276307",
            f"price_change_pct_{suffix}": "0.0000000000",
            f"price_change_{suffix}": "0.0000000000",
        }

    @property
    def no_trades_info(self):
        return []

    @property
    def trades_info(self):
        return [
            {
                "trade_id": "c76ed996-d44a-4e39-998e-acb68681b0f9",
                "price": "0.8000000000",
                "base_volume": "20",
                "quote_volume": "15",
                "timestamp": "1697471102",
                "type": "buy",
            },
            {
                "trade_id": "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d",
                "price": "1.0000000000",
                "base_volume": "20",
                "quote_volume": "20",
                "timestamp": "1697471080",
                "type": "buy",
            },
            {
                "trade_id": "d2602fa9-6680-42f9-9cb8-20f76275f587",
                "price": "1.2000000000",
                "base_volume": "20",
                "quote_volume": "24.5",
                "timestamp": "1697469503",
                "type": "buy",
            },
        ]

    @property
    def swaps_for_pair(self):
        [
            {
                "id": 432502,
                "uuid": "ee8d9212-ad7d-4865-a8de-4dc7fa70c1b8",
                "pair": "PND_ETC",
                "pair_std": "PND_ETC",
                "pair_reverse": "ETC_PND",
                "pair_std_reverse": "ETC_PND",
                "trade_type": "sell",
                "is_success": 1,
                "taker_amount": 16626.41292032,
                "taker_coin": "PND",
                "taker_coin_ticker": "PND",
                "taker_coin_platform": "",
                "taker_gui": "Komodo Wallet 0.9.0 Android; BT=1706085862",
                "taker_pubkey": "03cee1c12391f0360fbfef8da7fc5ad066c7b45124d518feccb67ea2c599b7741d",
                "taker_version": "2.0.0-beta_b0fd99e",
                "taker_coin_usd_price": 0,
                "maker_amount": 0.0277897648,
                "maker_coin": "ETC",
                "maker_coin_ticker": "ETC",
                "maker_coin_platform": "",
                "maker_gui": "Komodo Wallet 0.7.0-beta",
                "maker_pubkey": "029c6b0494d8edc112d671378093905dbe39e7e1bcbe940248fc09340ac291b729",
                "maker_version": "2.0.0-beta_b0fd99e",
                "maker_coin_usd_price": 0,
                "price": 598292.7855555427,
                "reverse_price": 0.0000016714227496444047,
                "started_at": 1706719199,
                "finished_at": 1706720405,
                "duration": 1206,
                "last_updated": 1706806812,
            },
            {
                "id": 432415,
                "uuid": "fa00aea3-a7a1-4eef-92a2-3cdd2df7ba86",
                "pair": "PND_ETC",
                "pair_std": "PND_ETC",
                "pair_reverse": "ETC_PND",
                "pair_std_reverse": "ETC_PND",
                "trade_type": "sell",
                "is_success": 1,
                "taker_amount": 37219.42949734,
                "taker_coin": "PND",
                "taker_coin_ticker": "PND",
                "taker_coin_platform": "",
                "taker_gui": "Komodo Wallet 0.9.0 Android; BT=1706085862",
                "taker_pubkey": "03cee1c12391f0360fbfef8da7fc5ad066c7b45124d518feccb67ea2c599b7741d",
                "taker_version": "2.0.0-beta_b0fd99e",
                "taker_coin_usd_price": 0,
                "maker_amount": 0.0622094011,
                "maker_coin": "ETC",
                "maker_coin_ticker": "ETC",
                "maker_coin_platform": "",
                "maker_gui": "Komodo Wallet 0.7.0-beta",
                "maker_pubkey": "029c6b0494d8edc112d671378093905dbe39e7e1bcbe940248fc09340ac291b729",
                "maker_version": "2.0.0-beta_b0fd99e",
                "maker_coin_usd_price": 0,
                "price": 598292.6936659091,
                "reverse_price": 598292.6936659091,
                "started_at": 1706640332,
                "finished_at": 1706640495,
                "duration": 163,
                "last_updated": 1706726486,
            },
            {
                "id": 432250,
                "uuid": "fa0bdf92-375d-41c8-af45-5aa11380bf25",
                "pair": "ETC_PND",
                "pair_std": "ETC_PND",
                "pair_reverse": "PND_ETC",
                "pair_std_reverse": "PND_ETC",
                "trade_type": "buy",
                "is_success": 1,
                "taker_amount": 44019.72165308,
                "taker_coin": "PND",
                "taker_coin_ticker": "PND",
                "taker_coin_platform": "",
                "taker_gui": "Komodo Wallet 0.9.0 Android; BT=1706085862",
                "taker_pubkey": "03cee1c12391f0360fbfef8da7fc5ad066c7b45124d518feccb67ea2c599b7741d",
                "taker_version": "2.0.0-beta_b0fd99e",
                "taker_coin_usd_price": 0,
                "maker_amount": 0.0735755641,
                "maker_coin": "ETC",
                "maker_coin_ticker": "ETC",
                "maker_coin_platform": "",
                "maker_gui": "Komodo Wallet 0.7.0-beta",
                "maker_pubkey": "029c6b0494d8edc112d671378093905dbe39e7e1bcbe940248fc09340ac291b729",
                "maker_version": "2.0.0-beta_b0fd99e",
                "maker_coin_usd_price": 0,
                "price": 598292.7164003917,
                "reverse_price": 598292.7164003917,
                "started_at": 1706459998,
                "finished_at": 1706460890,
                "duration": 892,
                "last_updated": 1706547744,
            },
        ]

    @property
    def orderbook_as_string(self):
        return {
            "pair": "KMD_DASH",
            "base": "KMD",
            "quote": "DASH",
            "timestamp": "1704125581",
            "asks": [
                {"price": "5", "volume": "4"},
                {"price": "4", "volume": "6"},
                {"price": "56", "volume": "335"},
            ],
            "bids": [
                {"price": "10", "volume": "9"},
                {"price": "8", "volume": "7"},
            ],
            "liquidity_usd": 3000000,
            "total_asks_base_vol": 9999.99,
            "total_bids_base_vol": 200000,
            "total_asks_quote_vol": 800000,
            "total_bids_quote_vol": 500000,
            "total_asks_base_usd": 900000,
            "total_bids_quote_usd": 800000,
        }

    @property
    def orderbook_as_coords(self):
        return {
            "pair": "KMD_DASH",
            "base": "KMD",
            "quote": "DASH",
            "timestamp": "1704125581",
            "asks": [["5", "4"], ["4", "6"], ["56", "335"]],
            "bids": [["10", "9"], ["8", "7"]],
            "liquidity_usd": 3000000,
            "total_asks_base_vol": 9999.99,
            "total_bids_base_vol": 200000,
            "total_asks_quote_vol": 800000,
            "total_bids_quote_vol": 500000,
            "total_asks_base_usd": 900000,
            "total_bids_quote_usd": 800000,
        }

    @property
    def historical_trades(self):
        return [
            {
                "trade_id": "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d",
                "price": "0.8000000000",
                "base_volume": "20",
                "quote_volume": "16",
                "timestamp": "1697471102",
                "type": "buy",
            },
            {
                "trade_id": "c76ed996-d44a-4e39-998e-acb68681b0f9",
                "price": "1.0000000000",
                "base_volume": "20",
                "quote_volume": "20",
                "timestamp": "1697471080",
                "type": "buy",
            },
            {
                "trade_id": "d2602fa9-6680-42f9-9cb8-20f76275f587",
                "price": "1.2000000000",
                "base_volume": "20",
                "quote_volume": "24",
                "timestamp": "1697469503",
                "type": "buy",
            },
            {
                "trade_id": "c80e9b57-406f-4f9c-8b41-79ff2623cc7a",
                "price": "1.0000000000",
                "base_volume": "10",
                "quote_volume": "10",
                "timestamp": "1697475729",
                "type": "sell",
            },
            {
                "trade_id": "09d72ac9-3e55-4e84-9f32-cf22b5b442ad",
                "price": "1.0000000000",
                "base_volume": "20",
                "quote_volume": "20",
                "timestamp": "1697448297",
                "type": "sell",
            },
        ]

    @property
    def historical_data(self):
        return {
            "ticker_id": "KMD_LTC",
            "start_time": "1697394564",
            "end_time": "1697480964",
            "limit": "100",
            "trades_count": "5",
            "sum_base_volume_buys": "60",
            "sum_base_volume_sells": "30",
            "sum_quote_volume_buys": "60",
            "sum_quote_volume_sells": "30",
            "average_price": "1",
            "buy": [
                {
                    "trade_id": "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d",
                    "price": "0.8000000000",
                    "base_volume": "20",
                    "quote_volume": "16",
                    "timestamp": "1697471102",
                    "type": "buy",
                },
                {
                    "trade_id": "c76ed996-d44a-4e39-998e-acb68681b0f9",
                    "price": "1.0000000000",
                    "base_volume": "20",
                    "quote_volume": "20",
                    "timestamp": "1697471080",
                    "type": "buy",
                },
                {
                    "trade_id": "d2602fa9-6680-42f9-9cb8-20f76275f587",
                    "price": "1.2000000000",
                    "base_volume": "20",
                    "quote_volume": "24",
                    "timestamp": "1697469503",
                    "type": "buy",
                },
            ],
            "sell": [
                {
                    "trade_id": "c80e9b57-406f-4f9c-8b41-79ff2623cc7a",
                    "price": "1.0000000000",
                    "base_volume": "10",
                    "quote_volume": "10",
                    "timestamp": "1697475729",
                    "type": "sell",
                },
                {
                    "trade_id": "09d72ac9-3e55-4e84-9f32-cf22b5b442ad",
                    "price": "1.0000000000",
                    "base_volume": "20",
                    "quote_volume": "20",
                    "timestamp": "1697448297",
                    "type": "sell",
                },
            ],
        }

    @property
    def dirty_dict(self):
        return {
            "a": Decimal("1.23456789"),
            "b": "string",
            "c": 1,
            "d": False,
            "e": ["foo", "bar"],
            "f": {"foo": "bar"},
        }


sampledata = SampleData()
