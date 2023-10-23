## A Simple API for AtomicDEX network statistics

**Currrent production URL:** https://defi-stats.komodo.earth/docs#/

The goal of this project is to provide all required data from the Komodo DeFi DEX network to match the required formats for [CoinGecko](https://docs.google.com/document/d/1v27QFoQq1SKT3Priq3aqPgB70Xd_PnDzbOCiuoCyixw/edit?usp=sharing)

Data is sourced from the [Komodo DeFi API's SQLite database](https://developers.komodoplatform.com/basic-docs/atomicdex/atomicdex-tutorials/query-the-mm2-database.html#my-swaps). It is calculated and then stored in json files in the `cache/`folder every 5 minutes (or less). API endpoints then serve the data from these files (to reduce API endpoint response times).

![image](https://user-images.githubusercontent.com/24797699/109954887-7030db00-7d14-11eb-9b4d-b384082c0705.png)


### Setup and requirements

- Run `./setup.sh` to generate `mm2/.env`, and install poetry dependencies
- Create `api/.env` file containing the following variables:

```
# FastAPI
API_PORT=8088
API_HOST='0.0.0.0'

# AtomicDEX API
MM2_DB_PATH='Path/To/MM2.db'
MM2_DB_HOST='atomic@stats-api.atomicdex.io'
MM2_DB_HOST_PATH='/DB/43ec929fe30ee72be42c9162c56dde910a05e50d/MM2.db'
API_PORT=7068
API_HOST='127.0.0.1'
POETRY_VERSION='1.6.1'
COINS_CONFIG_URL='https://raw.githubusercontent.com/KomodoPlatform/coins/master/utils/coins_config.json'
COINS_URL='https://raw.githubusercontent.com/KomodoPlatform/coins/master/coins'
```
- A maintained MM2.db file, ideally sourced from a long running AtomicDEX-API seed node to ensure all data is included. This is periodically sourced via rsync from `MM2_DB_HOST` (assuming ssh key access).


### Running the API
From the project root folder, execute `./run.sh`.
Optionally, use the `defi-stats.service` file as a template to let systemd to manage the defi stats service.

## Testing
From the `api` folder:
- To test everything: `pytest -vv`
- To test a specific file: `pytest -vv tests/test_file.py`


## Note
Alternative APIs are hosted at:
- https://stats-api.atomicdex.io/docs
- https://nomics.komodo.earth:8080/api/v1/info
- https://stats.testchain.xyz:8080/api/v1/summary

These should be consolidated in this repo at some point. They are based on branches of https://github.com/KomodoPlatform/dexstats_sqlite_py


## Endpoints

All endpoints for this update will have a `api/v3/` prefix. Swagger docs are available at https://192.168.0.1:7766/docs#/ (replace with domain/IP address when deployed).

For gecko endpoints, we are using the prefix `api/v3/gecko/`

