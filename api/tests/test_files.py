import os
import pytest
from util.files import Files
from util.urls import Urls

from util.helper import (
    get_mm2_rpc_port,
    get_netid_filename,
    get_chunks,
    get_price_at_finish,
    is_pair_priced,
)

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


files = Files(testing=True, netid="7777")
urls = Urls(testing=True, netid="7777")


def test_save_json():
    fn = files.get_cache_fn("foo")

    data = []
    resp = files.save_json(fn, data)
    assert resp["result"] == "error"

    data = 4311080719
    resp = files.save_json(fn, data)
    assert resp["result"] == "error"

    data = "hello world"
    resp = files.save_json(fn, data)
    assert resp["loglevel"] == "warning"

    data = {"hello": "world"}
    resp = files.save_json(fn, data)
    assert resp["result"] == "success"

    data = [{"hello": "world"}]
    resp = files.save_json(fn, data)
    assert resp["loglevel"] == "save"


def test_load_jsonfile():
    fn = files.get_cache_fn("foo")
    data = files.load_jsonfile(fn)
    assert "hello" in data[0]


def test_download_jsonfile():
    url = urls.get_cache_url("bar")
    data = files.download_json(url)
    assert "avatar_url" in data
    assert data["avatar_url"] == "https://avatars.githubusercontent.com/u/35845239?v=4"


def test_get_cache_fn():
    fn = files.get_cache_fn("fixer_rates")
    assert fn.endswith("fixer_io.json")