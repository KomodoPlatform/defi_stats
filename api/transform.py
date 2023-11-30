from logger import logger

def gecko_ticker_to_market_ticker(i):
    return {
        "trading_pair": f"{i['base_currency']}_{i['target_currency']}",
        "last_price": i['last_price'],
        "base_currency": i['base_currency'],
        "base_volume": i['base_volume'],
        "quote_currency": i['target_currency'],
        "quote_volume": i['target_volume'],
        "lowest_ask": i['ask'],
        "highest_bid": i['bid'],
        "price_change_percent_24h": i['price_change_percent_24h'],
        "highest_price_24h": i['high'],
        "lowest_price_24h": i['low'],
        "trades_24h": int(i['trades_24hr']),
        "last_swap_timestamp": int(i['last_trade'])
    }