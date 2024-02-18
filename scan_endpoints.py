#!/usr/bin/env python3
import time
import requests
import os
import sys

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
LOGGER_PATH = f"{ROOT_PATH}/api/util"
sys.path.append(LOGGER_PATH)

print(LOGGER_PATH)

from logger import logger


swagger = requests.get("http://0.0.0.0:7068/openapi.json").json()
endpoints = swagger["paths"].keys()

localhost = "http://0.0.0.0:7068"
test_domain = "https://test.defi-stats.komodo.earth"

def test_swagger_endpoints(path=None):
    for i in endpoints:
        if path is not None:
            if path not in i:
                continue
        i = i.replace("{ticker_id}", "KMD_LTC")
        i = i.replace("{pair_str}", "KMD_LTC")
        i = i.replace("{market_pair}", "KMD_LTC")
        i = i.replace("{days_in_past}", "3")
        i = i.replace("{uuid}", "82df2fc6-df0f-439a-a4d3-efb42a3c1db8")
        i = i.replace("{coin}", "KMD")
        i = i.replace("{ticker}", "KMD")
        i = i.replace("{category}", "version")
        
        if i.endswith("ticker_for_ticker"):
            i = f"{i}/?ticker=KMD"

        if i.endswith("/distinct"):
            i = f"{i}/?coin=KMD"
        i = f"{localhost}{i}"


        logger.merge(f"================== {i} ==================")
        r = requests.get(i)
        try:
            if r.status_code != 200:
                logger.warning(f"Status Code: {r.status_code}")
            else:
                logger.query(f"Status Code: {r.status_code}")
                
            if "error" in r.text:
                logger.warning(f"err: {r.text[:50]}")
                raise ValueError
            else:
                l = r.json()
                l_txt = r.text[:60]
                if isinstance(r.json(), list):
                    l = l[0]

                # Get test domain response
                r = requests.get(i.replace(localhost, test_domain))
                t = r.json()
                t_txt = r.text[:60]
                if isinstance(r.json(), list):
                    t = t[0]
                    
                local_keys = list(l.keys()) 
                test_keys = list(t.keys())
                logger.calc(f"Local:       {l_txt}")
                logger.calc(f"Local:       {local_keys[:10]}")
                logger.loop(f"Test domain: {t_txt}")
                logger.loop(f"Test domain: {test_keys[:10]}")
        except Exception as e:
            logger.calc(type(e))
            logger.calc(e)
            logger.warning(f"failed...")
            logger.warning(f"{e}: {r.text[:100]}")


if __name__ == "__main__":
    test_swagger_endpoints("markets")
