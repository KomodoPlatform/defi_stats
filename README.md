## A Simple API for AtomicDEX network statistics

**Currrent production URL:** https://stats-api.atomicdex.io/docs#/

The goal of this project is to provide all required data from AtomicDEX network to match the required formats for third party sites like CoinMarketCap, CoinGecko, CoinPaprika, etc. For example, the [CMC "Ideal endpoint"](https://docs.google.com/document/d/1S4urpzUnO2t7DmS_1dc4EL4tgnnbTObPYXvDeBnukCg/edit#) criteria.

Data is sourced from the [AtomicDEX-API SQLite database](https://developers.komodoplatform.com/basic-docs/atomicdex/atomicdex-tutorials/query-the-mm2-database.html#my-swaps). It is calculated and then stored in json files in the `cache/`folder every 5 minutes (or less). API endpoints then serve the data from these files (to reduce API endpoint response times).

![image](https://user-images.githubusercontent.com/24797699/109954887-7030db00-7d14-11eb-9b4d-b384082c0705.png)

### Requirements

- Python 3.7+
- Install pip packages with `pip3 install -r requirements.txt`
- An active [AtomicDEX-API](https://github.com/KomodoPlatform/atomicDEX-API) session (to query orderbooks)
- A maintained MM2.db file, ideally sourced from a long running AtomicDEX-API seed node to ensure all data is included.
- A `.env` file containing the following variables:

```
# FastAPI
API_PORT=8088
API_HOST='0.0.0.0'

# AtomicDEX API
MM2_USERPASS=Ent3r_Y@ur_Us3rP@ssw0rd_H3r3
MM2_DB_PATH='Path/To/MM2.db'
COINS_CONFIG_URL=https://raw.githubusercontent.com/KomodoPlatform/coins/master/utils/coins_config.json
COINS_URL=https://raw.githubusercontent.com/KomodoPlatform/coins/master/coins
```

## Testing

- To test everything: `pytest -v`
- To test a specific file: `pytest -v tests/test_file.py`


## Note
This is based on the `stats-api-atomicdex-io-update` branch, but endpoints from other branches should be merged in here also. 
The `stats-api-atomicdex-io` branch is currently deployed to production at https://stats-api.atomicdex.io/docs
The `nomics_api` branch is currently deployed to production at https://nomics.komodo.earth:8080/api/v1/info
The `markets_atomicdex` branch is currently deployed to production at https://stats.testchain.xyz:8080/api/v1/summary

## Endpoints

All endpoints for this update will have a `api/v3/` prefix. Swagger docs are available at https://192.168.0.1:7766/docs#/ (replace with domain/IP address when deployed).

#### Create network
docker network create --gateway 172.18.0.1 --subnet 172.18.0.0/24 stats-net