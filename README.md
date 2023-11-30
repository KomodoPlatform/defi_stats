## A Simple API for AtomicDEX network statistics

**Currrent production URL:** https://defi-stats.komodo.earth/docs#/

The goal of this project is to provide all required data from the Komodo DeFi DEX network to match the required formats for a variety of target consumers:
- [CoinGecko](https://docs.google.com/document/d/1v27QFoQq1SKT3Priq3aqPgB70Xd_PnDzbOCiuoCyixw/edit?usp=sharing)
- [CMC](https://docs.google.com/document/d/1S4urpzUnO2t7DmS_1dc4EL4tgnnbTObPYXvDeBnukCg/edit#)
- https://markets.atomicdex.io/

Data is sourced from the [Komodo DeFi API's SQLite database](https://developers.komodoplatform.com/basic-docs/atomicdex/atomicdex-tutorials/query-the-mm2-database.html#my-swaps). It is calculated and then stored in json files in the `cache/`folder every 5 minutes (or less). API endpoints then serve the data from these files (to reduce API endpoint response times).

Data is also imported into more robust databases (mySQL, postgresSQL/Timescale & dGraph) for future reference and to alow more complex queries over a longer timeframe.



### Setup and requirements

- Run `./setup.sh` to generate `mm2/.env`, and install poetry dependencies
- Create `api/.env` file containing the following variables:

```
TBA
```
- A maintained MM2.db file, ideally sourced from a long running AtomicDEX-API seed node to ensure all data is included. This is periodically sourced via rsync with `./update_MM2.db` (assuming ssh key access). This should be added to cron to check for updates every minute. E.g. `* * * * * /home/admin/defi_stats/update_MM2_db.sh`


### Running the API
From the project root folder, execute `./run.sh`.
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

For gecko endpoints, we are using the prefix `api/v3/gecko/`

