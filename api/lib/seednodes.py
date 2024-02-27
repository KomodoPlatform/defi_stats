#!/usr/bin/env python3
from decimal import Decimal
from functools import cached_property
from const import (
    MM2_DB_PATH_SEED,
)
from db.schema import Mm2StatsNodes
from lib.dex_api import DexAPI
from util.cron import cron
from util.logger import logger
import db.sqldb as db
import util.memcache as memcache


class SeedNode:
    def __init__(self) -> None:
        self.gecko_source = memcache.get_gecko_source()
        pass

    def get_active_mm2_versions(self, ts):
        active_versions = []
        for version in self.seednode_versions:
            if int(ts) < self.seednode_versions[version]["end"]:
                active_versions.append(version)
        return active_versions

    def is_mm2_version_valid(self, version, timestamp):
        active_versions = self.get_active_mm2_versions(timestamp)
        if version in active_versions:
            return True
        return False

    def get_seednode_stats(self, start_time=0, end_time=0):
        if start_time == 0:
            start_time = int(cron.now_utc()) - 3600
        if end_time == 0:
            end_time = int(cron.now_utc())
        logger.calc(MM2_DB_PATH_SEED)
        mm2_sqlite = db.SqlQuery(
            db_type="sqlite",
            db_path=MM2_DB_PATH_SEED,
            table=Mm2StatsNodes,
            gecko_source=self.gecko_source,
        )
        return mm2_sqlite.get_seednode_stats(start_time=start_time, end_time=end_time)

    @property
    def latest_data(self):
        mm2_sqlite = db.SqlQuery(
            db_type="sqlite",
            db_path=MM2_DB_PATH_SEED,
            table=Mm2StatsNodes,
            gecko_source=self.gecko_source,
        )
        return mm2_sqlite.get_latest_seednode_data()

    def register_notaries(self):
        dex = DexAPI()
        for notary in self.notary_seednodes:
            domain = self.notary_seednodes[notary]["domain"]
            peer_id = self.notary_seednodes[notary]["peer_id"]
            dex.add_seednode_for_stats(notary, domain, peer_id)

    @cached_property
    def seednode_versions(self):
        return {
            "b8598439a": {"end": 1659006671},
            "ce32ab8da": {"end": 1666271160},
            "0f6c72615": {"end": 1668898800},
            "6e4de5d21": {"end": 1680566399},
            "6bb79b3d8": {"end": 1680911999},
            "278b525ba": {"end": 1680911999},
            "371595d6c": {"version": "1.0.4", "end": 1688829251},
            "19c8218": {"version": "1.0.6", "end": 1700697600},
            "79f6205": {"version": "1.0.7", "end": 1705212928},
            "b0fd99e": {"version": "2.0.0", "end": 1987654321},
        }

    @cached_property
    def notary_seednodes(self):
        return {
            "alien_EU": {
                "domain": "alien-eu.techloverhd.com",
                "peer_id": "12D3KooWSCmjGYjmjEEiMYZyCZVuEYmGQCAtrMdpWcGSbGG39aHv",
            },
            "alien_NA": {
                "domain": "alien-na.techloverhd.com",
                "peer_id": "12D3KooWA9bym7s8gMdPVHcX872yjrz6Sq5rjpZAKBVFyoeWpJie",
            },
            "alien_SH": {
                "domain": "alien-sh.techloverhd.com",
                "peer_id": "12D3KooWBcVknefLZ3ZEfbFUHzfB2HzUjW4WLVDTe7TBqPmap9Cy",
            },
            "alienx_NA": {
                "domain": "alienx-na.techloverhd.com",
                "peer_id": "12D3KooWBXS7vcjYGQ5vy7nZj65FicpdxXsavPdLYB8gN7Ai3ruA",
            },
            "blackice_AR": {
                "domain": "shadowbit-ar.mm2.kmd.sh",
                "peer_id": "12D3KooWShhz3vfTqUXXVb9ivHeGBEEeMJvoda2ta8CVMhrX8RbZ",
            },
            "blackice_DEV": {
                "domain": "shadowbit-dev.mm2.kmd.sh",
                "peer_id": "12D3KooWDDZiyNn92StCdKXLLdxuYmkjJGPL5ezzyiJ2YVLMK56N",
            },
            "blackice_EU": {
                "domain": "shadowbit-eu.mm2.kmd.sh",
                "peer_id": "12D3KooWBT1UXwjqyavsDTVgWGeJkvrr8QgMScKpJF4oTLLgSk7k",
            },
            "chmex_AR": {
                "domain": "1.ar.seed.adex.dexstats.info",
                "peer_id": "12D3KooWD3uwYqzDygMvU3jaJozEXfZiiRFnkVVwUgpu9kGqa5Yg",
            },
            "chmex_EU": {
                "domain": "1.eu.seed.adex.dexstats.info",
                "peer_id": "12D3KooWGP4ryfJHXjfnbXUWP6FJeDLiif8jMT8obQvCKMSPUB8X",
            },
            "chmex_NA": {
                "domain": "1.na.seed.adex.dexstats.info",
                "peer_id": "12D3KooWDNUgDwAAuJbyoS5DiRbhvMSwrUh1yepKsJH8URcFwPp3",
            },
            "chmex_SH": {
                "domain": "1.sh.seed.adex.dexstats.info",
                "peer_id": "12D3KooWE8Ju9SZyZrfkUgi25gFKv1Yc6zcQZ5GXtEged8rmLW3t",
            },
            "cipi_AR": {
                "domain": "cipi_ar.cipig.net",
                "peer_id": "12D3KooWMsfmq3bNNPZTr7HdhTQvxovuR1jo5qvM362VQZorTk3F",
            },
            "cipi_EU": {
                "domain": "cipi_eu.cipig.net",
                "peer_id": "12D3KooWBhGrTVfaK9v12eA3Et84Y8Bc6ixfZVVGShsad2GBWzm3",
            },
            "cipi_NA": {
                "domain": "cipi_na.cipig.net",
                "peer_id": "12D3KooWBoQYTPf4q2bnsw8fUA2LKoknccVLrAcF1caCa48ev8QU",
            },
            "caglarkaya_EU": {
                "domain": "eu.caglarkaya.net",
                "peer_id": "12D3KooWEg7MBp1P9k9rYVBcW5pa8tsHhyE5UuGAAerCARLzZBPn",
            },
            "computergenie_EU": {
                "domain": "cgeu.computergenie.gay",
                "peer_id": "12D3KooWGkPFi43Nq6cAcc3gib1iECZijnKZLgEf1q1MBRKLczJF",
            },
            "computergenie_NA": {
                "domain": "cg.computergenie.gay",
                "peer_id": "12D3KooWCJWT5PAG1jdYHyMnoDcxBKMpPrUVi9gwSvVLjLUGmtQg",
            },
            "dragonhound_AR": {
                "domain": "ar.smk.dog",
                "peer_id": "12D3KooWSUABQ2beSQW2nXLiqn4DtfXyqbJQDd2SvmgoVwXjrd9c",
            },
            "dragonhound_DEV": {
                "domain": "dev.smk.dog",
                "peer_id": "12D3KooWNGGBfPWQbubupECdkYhj1VomMLUUAYpsR2Bo3R4NzHju",
            },
            "dragonhound_EU": {
                "domain": "s7eu.smk.dog",
                "peer_id": "12D3KooWDgFfyAzbuYNLMzMaZT9zBJX9EHd38XLQDRbNDYAYqMzd",
            },
            "dragonhound_NA": {
                "domain": "s7na.smk.dog",
                "peer_id": "12D3KooWSmizY35qrfwX8qsuo8H8qrrvDjXBTMRBfeYsRQoybHaA",
            },
            "fediakash_AR": {
                "domain": "fediakash.mooo.com",
                "peer_id": "12D3KooWCSidNncnbDXrX5G6uWdFdCBrMpaCAqtNxSyfUcZgwF7t",
            },
            "gcharang_DEV": {
                "domain": "mm-dev.lordofthechains.com",
                "peer_id": "12D3KooWMEwnQMPUHcGw65xMmhs1Aoc8WSEfCqTa9fFx2Y3PM9xg",
            },
            "gcharang_SH": {
                "domain": "mm-sh.lordofthechains.com",
                "peer_id": "12D3KooWHAk9eJ78pwbopZMeHMhCEhXbph3CJ8Hbz5L1KWTmPf8C",
            },
            "gcharang_AR": {
                "domain": "mm-ar.lordofthechains.com",
                "peer_id": "12D3KooWDsFMoRoL5A4ii3UonuQZ9Ti2hrc7PpytRrct2Fg8GRq9",
            },
            "mcrypt_SH": {
                "domain": "mcrypt2.v6.rocks",
                "peer_id": "12D3KooWCDAPYXtNzC3x9kYuZySSf1WtxjGgasxapHEdFWs8Bep3",
            },
            "nodeone_NA": {
                "domain": "nodeone.computergenie.gay",
                "peer_id": "12D3KooWBTNDr6ih5efzVSxXtDv9wcVxHNj8RCvUnpKfKb6eUYet",
            },
            "sheeba_SH": {
                "domain": "sheeba.computergenie.gay",
                "peer_id": "12D3KooWC1P69a5TwpNisZYBXRgkrJDjGfn4QZ2L4nHZDGjcdR2N",
            },
            "smdmitry_AR": {
                "domain": "mm2-smdmitry-ar.smdmitry.com",
                "peer_id": "12D3KooWJ3dEWK7ym1uwc5SmwbmfFSRmELrA9aPJYxFRrQCCNdwF",
            },
            "smdmitry2_AR": {
                "domain": "mm2-smdmitry2-ar.smdmitry.com",
                "peer_id": "12D3KooWEpiMuCc47cYUXiLY5LcEEesREUNpZXF6KZA8jmFgxAeE",
            },
            "smdmitry_EU": {
                "domain": "mm2-smdmitry-eu.smdmitry.com",
                "peer_id": "12D3KooWJTYiU9CqVyycpMnGC96WyP1GE62Ng5g93AUe9wRx5g7W",
            },
            "smdmitry_SH": {
                "domain": "mm2-smdmitry-sh.smdmitry.com",
                "peer_id": "12D3KooWQP7PNNX5DSyhPX5igPQKQhet4KX7YaDqiGuNnarr4vRX",
            },
            "strob_SH": {
                "domain": "sh.strobfx.com",
                "peer_id": "12D3KooWFY5TmKpusUJ3jJBYK4va8xQchnJ6yyxCD7wZ2pWVK23p",
            },
            "tonyl_AR": {
                "domain": "ar.farting.pro",
                "peer_id": "12D3KooWEMTeavnNtPPYr1u4aPFB6U39kdMD32SU1EpHGWqMpUJk",
            },
            "tonyl_DEV": {
                "domain": "dev.farting.pro",
                "peer_id": "12D3KooWDubAUWDP2PgUXHjEdN3SGnkszcyUgahALFvaxgp9Jcyt",
            },
            "van_EU": {
                "domain": "van.computergenie.gay",
                "peer_id": "12D3KooWMX4hEznkanh4bTShzCZNx8JJkvGLETYtdVw8CWSaTUfQ",
            },
            "webworker01_EU": {
                "domain": "eu2.webworker.sh",
                "peer_id": "12D3KooWGF5siktvWLtXoRKgbzPYHn4rib9Fu8HHJEECRcNbNoAs",
            },
            "webworker01_NA": {
                "domain": "na2.webworker.sh",
                "peer_id": "12D3KooWRiv4gFUUSy2772YTagkZYdVkjLwiXkdcrtDQQuEqQaJ9",
            },
            "who-biz_NA": {
                "domain": "adex.blur.cash",
                "peer_id": "12D3KooWQp97gsRE5LbcUPjZcP7N6qqk2YbxJmPRUDeKVM5tbcQH",
            },
        }


seednode = SeedNode()
