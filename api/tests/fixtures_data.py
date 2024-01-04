from decimal import Decimal


historical_trades = [
    {
        "trade_id": "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d",
        "price": "0.8000000000",
        "base_volume": "20",
        "target_volume": "16",
        "timestamp": "1697471102",
        "type": "buy",
    },
    {
        "trade_id": "c76ed996-d44a-4e39-998e-acb68681b0f9",
        "price": "1.0000000000",
        "base_volume": "20",
        "target_volume": "20",
        "timestamp": "1697471080",
        "type": "buy",
    },
    {
        "trade_id": "d2602fa9-6680-42f9-9cb8-20f76275f587",
        "price": "1.2000000000",
        "base_volume": "20",
        "target_volume": "24",
        "timestamp": "1697469503",
        "type": "buy",
    },
    {
        "trade_id": "c80e9b57-406f-4f9c-8b41-79ff2623cc7a",
        "price": "1.0000000000",
        "base_volume": "10",
        "target_volume": "10",
        "timestamp": "1697475729",
        "type": "sell",
    },
    {
        "trade_id": "09d72ac9-3e55-4e84-9f32-cf22b5b442ad",
        "price": "1.0000000000",
        "base_volume": "20",
        "target_volume": "20",
        "timestamp": "1697448297",
        "type": "sell",
    },
]


historical_data = {
    "ticker_id": "KMD_LTC",
    "start_time": "1697394564",
    "end_time": "1697480964",
    "limit": "100",
    "trades_count": "5",
    "sum_base_volume_buys": "60",
    "sum_base_volume_sells": "30",
    "sum_target_volume_buys": "60",
    "sum_target_volume_sells": "30",
    "average_price": "1",
    "buy": [
        {
            "trade_id": "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d",
            "price": "0.8000000000",
            "base_volume": "20",
            "target_volume": "16",
            "timestamp": "1697471102",
            "type": "buy",
        },
        {
            "trade_id": "c76ed996-d44a-4e39-998e-acb68681b0f9",
            "price": "1.0000000000",
            "base_volume": "20",
            "target_volume": "20",
            "timestamp": "1697471080",
            "type": "buy",
        },
        {
            "trade_id": "d2602fa9-6680-42f9-9cb8-20f76275f587",
            "price": "1.2000000000",
            "base_volume": "20",
            "target_volume": "24",
            "timestamp": "1697469503",
            "type": "buy",
        },
    ],
    "sell": [
        {
            "trade_id": "c80e9b57-406f-4f9c-8b41-79ff2623cc7a",
            "price": "1.0000000000",
            "base_volume": "10",
            "target_volume": "10",
            "timestamp": "1697475729",
            "type": "sell",
        },
        {
            "trade_id": "09d72ac9-3e55-4e84-9f32-cf22b5b442ad",
            "price": "1.0000000000",
            "base_volume": "20",
            "target_volume": "20",
            "timestamp": "1697448297",
            "type": "sell",
        },
    ],
}


dirty_dict = {
    "a": Decimal("1.23456789"),
    "b": "string",
    "c": 1,
    "d": False,
    "e": ["foo", "bar"],
    "f": {"foo": "bar"},
}


coins_config = {
    "NOSWAP": {"wallet_only": True, "is_testnet": False},
    "TEST": {"wallet_only": False, "is_testnet": True},
    "OK": {"wallet_only": False, "is_testnet": False},
}


trades_info = [
    {
        "trade_id": "c76ed996-d44a-4e39-998e-acb68681b0f9",
        "price": "0.8000000000",
        "base_volume": "20",
        "target_volume": "15",
        "timestamp": "1697471102",
        "type": "buy",
    },
    {
        "trade_id": "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d",
        "price": "1.0000000000",
        "base_volume": "20",
        "target_volume": "20",
        "timestamp": "1697471080",
        "type": "buy",
    },
    {
        "trade_id": "d2602fa9-6680-42f9-9cb8-20f76275f587",
        "price": "1.2000000000",
        "base_volume": "20",
        "target_volume": "24.5",
        "timestamp": "1697469503",
        "type": "buy",
    },
]


ticker_item = {
    "ticker_id": "KMD_LTC",
    "pool_id": "KMD_LTC",
    "base_currency": "KMD",
    "target_currency": "LTC",
    "last_price": "0.0000000000",
    "last_trade": "0",
    "trades_24hr": "0",
    "base_volume": "0.0000000000",
    "target_volume": "0.0000000000",
    "base_usd_price": "0.3000000000",
    "target_usd_price": "75.0000000000",
    "bid": "0.0000000000",
    "ask": "0.0000000000",
    "high": "0.0000000000",
    "low": "0.0000000000",
    "volume_usd_24hr": "0.0000000000",
    "liquidity_in_usd": "0.0000000000",
    "price_change_percent_24h": 0,
    "price_change_24h": 0,
}


no_trades_info = []


swap_item = {
    "maker_coin": "KMD",
    "taker_coin": "LTC-segwit",
    "uuid": "7d36be55-6db3-4662-93af-74dc73a58bfa",
    "started_at": "1700000776",
    "finished_at": "1700000777",
    "maker_amount": "5",
    "taker_amount": "4",
    "is_success": "1",
    "maker_coin_ticker": "KMD",
    "maker_coin_platform": "",
    "taker_coin_ticker": "LTC",
    "taker_coin_platform": "segwit",
    "maker_coin_usd_price": "0.45",
    "taker_coin_usd_price": "75.1",
}


swap_item2 = {
    "maker_coin": "KMD",
    "taker_coin": "LTC-segwit",
    "uuid": "7d36be55-6db3-4662-93af-74dc73a58bfa",
    "started_at": "1700000776",
    "finished_at": "1700000000",
    "maker_amount": "5",
    "taker_amount": "4",
    "is_success": "0",
    "maker_coin_ticker": "KMD",
    "maker_coin_platform": "",
    "taker_coin_ticker": "LTC",
    "taker_coin_platform": "segwit",
    "maker_coin_usd_price": "0.5",
    "taker_coin_usd_price": "0",
}


valid_tickers = ["KMD_LTC", "KMD_DASH", "KMD_BTC"]


orderbook_as_string = {
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


orderbook_as_coords = {
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
