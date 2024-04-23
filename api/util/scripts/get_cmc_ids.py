#!/usr/bin/env python3
import os
import sys
import json
import requests

'''
A simple script to compare cmc/assets_source.json with coins/coins_config.json 
to extract the unified assets ID for CMC 'assets' endpoint.
'''


'''
Desired output format (other fields are static defined by 'CmcAsset' model)
   "BTC":{  
      "name":"bitcoin",
      "unified_cryptoasset_id" :"1",
   }
'''
SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
API_ROOT_PATH = os.path.dirname(os.path.dirname(SCRIPT_PATH))
PROJECT_ROOT_PATH = os.path.dirname(API_ROOT_PATH)

COINS_CONFIG_URL='https://raw.githubusercontent.com/KomodoPlatform/coins/master/utils/coins_config.json'


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
    return {i["symbol"]: i for i in data}
    
def get_common_keys(coins_config, cmc_assets):
    coins_config_keys = list(set([i.split("-")[0] for i in coins_config.keys() if coins_config[i]["is_testnet"] is False]))
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
        if 'explorer_url' in coins_config_info:
            explorer = coins_config_info['explorer_url']
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
                decimals = coins_config_info['protocol']['protocol_data']['decimals']
                link = f"{explorer}/#/tokens/{contract}?type={decimals}"
            case _:
                print(f"Platform {platform} not covered!")
    except Exception as e:
        print(e)
    return link
        

def extract_ids(recent_only=True):
    coins_config = get_coins_config()
    cmc_assets = get_cmc_assets()
    recent_traded_coins_14d = get_recent_traded_coins_14d()
    data = []
    for coin in get_common_keys(coins_config, cmc_assets):
        if recent_only and coin not in recent_traded_coins_14d:
            continue
        src = cmc_assets[coin]
        item = {
            coin: {
                "name": src['name'],
                "unified_cryptoasset_id": src['id']
            }
        }
        for c in coins_config:
            try:
                if c == "BCH":
                    # BCH is parent of SLP tokens, but lacks some protocol keys so we ignore it
                    continue
                if c.split("-")[0] == coin:
                    # We need to check manually if tickers are representative of the same coin
                    print(f"{src['name']} | {coins_config[c]['name']} | {coins_config[c]['fname']}")
                    if 'protocol' in coins_config[c]:
                        if 'protocol_data' in coins_config[c]['protocol']:
                            platform = coins_config[c]['protocol']['protocol_data']['platform']
                            if platform == "BCH":
                                contract = coins_config[c]['protocol']['protocol_data']['token_id']
                            elif platform in ["IRIS"]:
                                contract = coins_config[c]['protocol']['protocol_data']['denom'].replace("ibc/", "")
                            else:
                                contract = coins_config[c]['protocol']['protocol_data']['contract_address']
                            contract_link = get_contract_link(platform, contract, coins_config[c])
                            # print(f"{c}: {contract_link}")
                            if contract_link is not None:
                                for i in ["contractAddressUrl", "contractAddress"]:
                                    if i not in item[coin]:
                                        item[coin].update({i: []})
                            item[coin]["contractAddress"].append(contract)
                            item[coin]["contractAddressUrl"].append(contract_link)
            except Exception as e:
                if c not in ["ARRR", "ATOM", "IRIS", "OSMO"]:
                    print(f"Error getting link for {c}: {e}")
        for i in ["contractAddressUrl", "contractAddress"]:
            if i in item[coin]:
                item[coin][i] = ','.join(item[coin][i])
        data.append(item)
    if recent_only:
        print(f"Filtered down to {len(data)} coins after excluding not recently traded")
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





data = extract_ids()
with open("cmc_ids.json", "w+") as f:
    json.dump(data, f, indent=4)
    