## A Simple API for AtomicDEX network statistics

**Currrent production URL:** https://defi-stats.komodo.earth/docs#/

The goal of this project is to provide all required data from the Komodo DeFi DEX network to match the required formats for a variety of target consumers:
- [CoinGecko](https://docs.google.com/document/d/1v27QFoQq1SKT3Priq3aqPgB70Xd_PnDzbOCiuoCyixw/edit?usp=sharing)
- [CMC](https://docs.google.com/document/d/1S4urpzUnO2t7DmS_1dc4EL4tgnnbTObPYXvDeBnukCg/edit#)
- https://markets.atomicdex.io/

Data is sourced from the [Komodo DeFi API's SQLite database](https://developers.komodoplatform.com/basic-docs/atomicdex/atomicdex-tutorials/query-the-mm2-database.html#my-swaps). It is calculated and then stored in json files in the `cache/`folder every 5 minutes (or less). API endpoints then serve the data from these files (to reduce API endpoint response times).

![image](https://github.com/KomodoPlatform/defi_stats/assets/35845239/106973e9-72ca-4486-9fc5-ce7e7e1bfe57)

Data is also imported into more robust databases (mySQL, postgresSQL/Timescale & dGraph) for future reference and to alow more complex queries over a longer timeframe.


### Setup and requirements

- Run `./setup.sh` to install poetry dependencies
- Create `mm2/.env`, and `mm2_8762/.env` with the following inside (change the userpass)
```
MM2_CONF_PATH=/home/komodian/mm2/MM2.json
MM_COINS_PATH=/home/komodian/mm2/coins
MM_LOG=/home/komodian/mm2/mm2.log
USERPASS=RPC_CONTRoL_USERP@SSW0RD
USER_ID=1000
GROUP_ID=1000
```
- Next, create `mm2/MM2.json` and `mm2_8762/MM2.json`. Use the templates in each folder - `cp mm2/MM2.template.json mm2/MM2.json`. Change the passphrases.

- Get the latest coins file for each mm2 folder with `wget https://raw.githubusercontent.com/KomodoPlatform/coins/master/coins`

- Then execute `./run_mm2.sh` to initialise the mm2 databases. You will need to find the path to the MM2.db file in each folder to input below. This script will tail the logs, but you can `ctl-c` to exit without stopping the service.

- Create `api/.env` file containing the following variables:

```
# DEPENDENCY VERSIONS
POETRY_VERSION='1.6.1'

# FastAPI
API_PORT=7068
API_HOST='0.0.0.0'
API_USER="komodian"
API_PASS="api_password"

# AtomicDEX API
COINS_CONFIG_URL='https://raw.githubusercontent.com/KomodoPlatform/coins/master/utils/coins_config.json'
COINS_URL='https://raw.githubusercontent.com/KomodoPlatform/coins/master/coins'



# API KEYS
FIXER_API_KEY=your_key

# DATABASES

## External MySQL source DB
mysql_hostname="db_IP"
mysql_username="db_username"
mysql_password="db_password"
mysql_db="db_name"

## Postgres/TimescaleDB
POSTGRES_HOST=timescale
POSTGRES_PORT=5432
POSTGRES_DATABASE=db_name
PGDATA=/var/lib/postgresql/data
POSTGRES_USER=db_user
POSTGRES_PASSWORD=db_pass

## Sqlite
LOCAL_MM2_DB_PATH_7777="/home/admin/defi_stats/mm2/DB/db_hex/MM2.db"
LOCAL_MM2_DB_PATH_8762="/home/admin/defi_stats/mm2_8762/DB/db_hex/MM2.db"

```

Edit the values for your paths and passwords etc. Some of these are not curently in us in main branch, but will be used later.

# Sourcing data

- To ensure data integrity, past swaps are sourced from several long running Seed Nodes. This is periodically sourced via rsync with `./update_MM2.db` (assuming ssh key access). This script should be added to cron to check for updates every minute. E.g. `* * * * * /home/admin/defi_stats/update_MM2_db.sh`


### Running the API
From the project root folder, execute `./run_api.sh`.
Optionally, use the `defi-stats.service` file as a template to let systemd to manage the defi stats service.

## Testing

You may need to install pytest: `sudo apt install python-pytest`

From the `api` folder:
- To test everything: `poetry run pytest -vv`
- To test a specific file: `poetry run pytest -vv tests/test_file.py`


## Note
Alternative APIs are hosted at:
- https://stats-api.atomicdex.io/docs
- https://nomics.komodo.earth:8080/api/v1/info
- https://stats.testchain.xyz:8080/api/v1/summary

These should be consolidated in this repo at some point. They are based on branches of https://github.com/KomodoPlatform/dexstats_sqlite_py


## Endpoints

All endpoints for this update will have a `api/v3/` prefix. Swagger docs are available at https://192.168.0.1:7766/docs#/ (replace with domain/IP address when deployed).

For [CoinGecko](https://www.coingecko.com/) endpoints, we are using the prefix `api/v3/gecko/`
Endpoints previously at http://stats.testchain.xyz/ have been migrated to the prefix `api/v3/markets/`

## Data completeness
Add `* * * * * /home/USERNAME/defi_stats/update_MM2_db.sh > /home/atomic/logs/db_update.log` to the crontab of the server you are running this api on to collect a variety of MM2.db files on varying netids, and to cover any missing data from swaps completed during server downtime. SSH key access is required.

This will place the external MM2.db copies into the `defi_stats/DB` folder, which is then periodically scanned, with all data merged into `defi_stats/DB/{netid}_MM2.db` for each netid, and `defi_stats/DB/all_MM2.db` for the complete picture.
