#!/usr/bin/env python3
import os
import csv
import sys
import json
import requests

"""
A simple script to compare cmc/assets_source.json with coins/coins_config.json 
to extract the unified assets ID for CMC 'assets' endpoint.
"""


"""
Desired output format (other fields are static defined by 'CmcAsset' model)
   "BTC":{  
      "name":"bitcoin",
      "unified_cryptoasset_id" :"1",
   }
"""
SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
API_ROOT_PATH = os.path.dirname(os.path.dirname(SCRIPT_PATH))
PROJECT_ROOT_PATH = os.path.dirname(API_ROOT_PATH)

COINS_CONFIG_URL = "https://raw.githubusercontent.com/KomodoPlatform/coins/master/utils/coins_config.json"


def get_coins_config():
    return requests.get(COINS_CONFIG_URL).json()


def get_cmc_assets():
    apikey = "UNIFIED-CRYPTOASSET-INDEX"
    base_url = "https://pro-api.coinmarketcap.com"
    endpoint = f"v1/cryptocurrency/map?CMC_PRO_API_KEY={apikey}&listing_status=active"
    url = f"{base_url}/{endpoint}"
    print(url)
    r = requests.get(url)
    data = r.json()["data"]
    return data


def get_common_keys(coins_config, cmc_assets):
    coins_config_keys = list(
        set(
            [
                i.split("-")[0]
                for i in coins_config.keys()
                if coins_config[i]["is_testnet"] is False
            ]
        )
    )
    print(f"{len(coins_config_keys)} coins from coins_config.json")
    cmc_assets_keys = list(cmc_assets.keys())
    print(f"{len(cmc_assets_keys)} coins from CMC")
    data = [i for i in coins_config_keys if i in cmc_assets_keys]
    data.sort()
    print(f"{len(data)} common tickers")
    return data


def get_contract_link(platform, contract, coins_config_info, decimals=6):
    link = None
    try:
        if "explorer_url" in coins_config_info:
            explorer = coins_config_info["explorer_url"]
        else:
            print(f"No explorer defined for {platform}!")
        if explorer.endswith("/"):
            explorer = explorer[:-1]
        match platform:
            case "AVAX":
                link = f"{explorer}/token/{contract}"
            case "MATIC":
                link = f"{explorer}/token/{contract}"
            case "BNB":
                link = f"{explorer}/token/{contract}"
            case "ETH":
                link = f"{explorer}/token/{contract}"
            case "KCS":
                link = f"{explorer}/token/{contract}"
            case "FTM":
                link = f"{explorer}/token/{contract}"
            case "HT":
                link = f"{explorer}/token/{contract}"
            case "MOVR":
                link = f"{explorer}/token/{contract}"
            case "ETH-ARB20":
                link = f"{explorer}/token/{contract}"
            case "QTUM":
                link = f"{explorer}/token/{contract}"
            case "BCH":
                link = f"{explorer}/token/{contract}"
            case "IRIS":
                contract = contract.replace("ibc/", "")
                decimals = coins_config_info["protocol"]["protocol_data"]["decimals"]
                link = f"{explorer}/#/tokens/{contract}?type={decimals}"
            case _:
                print(f"Platform {platform} not covered!")
    except Exception as e:
        print(e)
    return link


def extract_ids(cmc_by_ticker, recent_only=False):
    coins_config = get_coins_config()
    common_keys = get_common_keys(coins_config, cmc_by_ticker)
    recent_traded_coins_14d = get_recent_traded_coins_14d()
    data = {}

    # These are ignored as known to not be in coins repo
    cmc_reject = [
        "Tracer",
        "MetaTrace",
        "Evernode",
        "ENEFTIVERSE",
        "D-SHOP",
        "Betero",
        "McLaren F1 Fan Token",
        "LABS Group",
        "PUTinCoin",
        "PUTinCoin",
        "PUTinCoin",
        "PUTinCoin",
        "Solar",
        "Xave Coin",
        "Ninja Protocol",
        "Dog Wif Nunchucks",
        "Shinobi",
        "Dot Finance",
        "Drawshop Kingdom Reverse",
        "Lynex",
        "Pink",
        "Joystream"
    ]
    # These have been sighted as the same token, though having a slightly different full name value
    cmc_allow = [
        "Pirate Chain",
        "KuCoin Token",
        "Jarvis Synthetic Euro",
        "Gleec Coin",
        "IRISnet",
        "BNB",
        "BUSD"
        "KuCoin Token",
        "Energy Web Token",
        "Injective",
        "VerusCoin",
        "KuCoin Token",
        "Loop Network",
        "GM Wagmi",
        "BIDR",
        "Energo",
        "FLOKI",
        "KuCoin Token",
        "Rootstock Smart Bitcoin",
        "CAKE",
        "GlobalBoost",
        "Hyper Pay"
    ]
    # These are for cases where there is a name match, but also mismatches which have been sighted as invalid
    coin_compare = [
        [
            "Coins Repo Ticker",
            "Base Ticker",
            "CMC Name",
            "Coins Repo Name",
            "Coins Repo Fname"
        ]
    ]
    # These are false positives in bad match detection
    ignore = ["DOGE", "AUR", "AVN", "ETH", "FLUX", "GLC", "GRS", "MONA", "ONE", "HT", "KOIN", "THC", "VAL", "VIA", "XVG", "POT", "PPC"]
    for coin in common_keys:
        needs_validation = True
        for coin_full in coins_config:
            if coin != coin_full.split("-")[0]:
                continue
            if recent_only and coin not in recent_traded_coins_14d:
                # print(f"Skipping {coin}, not recently traded")
                continue
            for src in cmc_by_ticker[coin]:
                # We need to check manually if tickers are representative of the same coin
                if src["name"] in cmc_reject:
                    # print(f"Skipping {src['name']}, in cmc ignore list")
                    continue
                if coin in data:
                    item = data[coin]
                else:
                    item = {"name": src["name"], "unified_cryptoasset_id": src["id"]}
                try:
                    if coin_full.endswith("_OLD"):
                        # Excluding these, though there may be a CMC ticker for them
                        continue
                    # If same name and symbol, we can be confident it is valid
                    if src["platform"] is not None and coin not in ["KCS"]:
                        token_address = src["platform"]["token_address"]
                        p = coins_config[coin_full]["protocol"]
                        if "protocol" in coins_config[coin_full]:
                            if ("protocol_data" in p):
                                protocol_data = p["protocol_data"]
                                platform = protocol_data["platform"]
                                if platform == "BCH":
                                    contract = protocol_data["token_id"]
                                elif platform in ["IRIS"]:
                                    contract = protocol_data["denom"]
                                else:
                                    contract = protocol_data["contract_address"]
                                if contract.lower() == token_address.lower() or src["name"] in cmc_allow:
                                    # print(f"Match on {coin}")
                                    contract_link = get_contract_link(
                                        platform, contract, coins_config[coin_full]
                                    )
                                    # print(f"{c}: {contract_link}")
                                    if contract_link is not None:
                                        for i in [
                                            "contractAddressUrl",
                                            "contractAddress",
                                        ]:
                                            if i not in item:
                                                item.update({i: []})

                                    item["contractAddress"].append(contract)
                                    item["contractAddressUrl"].append(contract_link)
                                    needs_validation = False
                                    # print(f"Valid {coin} found")
                            elif coin not in ignore:
                                print(f"CMC has platform but dex doesnt for {coin}")
                                    
                    elif (
                        src["name"].lower() == coins_config[coin_full]["name"].lower()
                        or src["name"].lower()
                        == coins_config[coin_full]["fname"].lower()
                        or src["name"] in cmc_allow
                    ):
                        needs_validation = False
                    
                    if needs_validation:
                        # print(f"Need to validate {coin_full}, not in data")
                        coin_compare.append(
                            [
                                coin_full,
                                coin,
                                src["name"],
                                coins_config[coin_full]["name"],
                                coins_config[coin_full]["fname"],
                            ]
                        )
                    elif coin not in data:
                        data.update({coin: item})
                    else:
                        data[coin].update(item)

                except Exception as e:
                    if coin not in ["ARRR", "ATOM", "IRIS", "OSMO"]:
                        print(f"Error getting link for {coin}: {e}")
    for coin in data:
        for i in ["contractAddressUrl", "contractAddress"]:
            if i in data[coin]:
                data[coin][i] = list(set(data[coin][i]))
                data[coin][i] = ",".join(data[coin][i])
    if recent_only:
        print(f"Filtered down to {len(data)} coins after excluding not recently traded")
    else:
        print(f"{len(data)} coins translated")

    with open("coin_compare.csv", mode="w") as cf:
        cw = csv.writer(cf, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for i in coin_compare:
            if i[1] not in data:
                cw.writerow(i)
    return data


def get_recent_traded_coins_14d():
    with open(f"{API_ROOT_PATH}/cache/pairs/volumes_14d.json", "r") as f:
        data = json.load(f)
        coins = []
        recent_pairs = list(data["data"]["volumes"].keys())
        for i in recent_pairs:
            for j in i.split("_"):
                coins.append(j)
        coins = list(set(coins))
        coins.sort()
        print(f"{len(coins)} coins traded in last 14 days")
        return coins


def get_cmc_by_ticker():
    data = get_cmc_assets()
    by_ticker = {}
    for i in data:
        if i["symbol"] not in by_ticker.keys():
            by_ticker.update({i["symbol"]: []})
        by_ticker[i["symbol"]].append(i)
    return by_ticker


cmc_by_ticker = get_cmc_by_ticker()
with open("cmc_ids_by_ticker.json", "w+") as f:
    json.dump(cmc_by_ticker, f, indent=4)


data = extract_ids(cmc_by_ticker)
with open("cmc_ids.json", "w+") as f:
    json.dump(data, f, indent=4)
