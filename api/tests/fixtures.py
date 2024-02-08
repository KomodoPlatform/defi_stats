#!/usr/bin/env python3
import pytest
from lib.stats_api import StatsAPI
import util.helper as helper


@pytest.fixture
def setup_statsapi():
    yield StatsAPI()
