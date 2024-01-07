import pytest
from decimal import Decimal
from fixtures_coin import (
    setup_coin_btc,
    setup_coin_btc_bep20,
    setup_coin_ltc,
    setup_coin_ltc_segwit,
    setup_coin,
    setup_coin_doc,
    setup_coin_atom,
    setup_get_gecko_price_and_mcap_doc,
    setup_get_gecko_price_and_mcap_ltc,
    setup_coin_bad,
)


def test_coin(
    setup_coin_btc,
    setup_coin_btc_bep20,
    setup_coin_ltc,
    setup_coin_ltc_segwit,
    setup_coin,
    setup_coin_doc,
    setup_coin_atom,
    setup_coin_bad,
):
    assert setup_coin_bad.type == "Delisted"
    assert not setup_coin_bad.is_testnet
    assert setup_coin_bad.is_wallet_only
    assert setup_coin_doc.is_testnet
    assert setup_coin_doc.is_valid
    assert not setup_coin_ltc.is_testnet
    assert setup_coin_ltc.has_segwit
    assert setup_coin_ltc_segwit.has_segwit
    assert not setup_coin_doc.has_segwit
    assert setup_coin_atom.is_wallet_only
    assert not setup_coin_atom.is_valid
    assert not setup_coin_ltc.is_tradable
    assert not setup_coin_atom.is_tradable
    assert setup_coin_ltc.is_valid
    assert not setup_coin_doc.is_wallet_only
    assert setup_coin_ltc_segwit.coin == "LTC-segwit"
    assert setup_coin_ltc_segwit.ticker == "LTC"
    assert setup_coin_ltc_segwit.type == "UTXO"
    assert setup_coin_btc_bep20.type == "BEP-20"

    assert setup_coin_doc.usd_price == 0
    assert setup_coin_doc.mcap == 0
    assert setup_coin_ltc.mcap > 0
    assert setup_coin_ltc.usd_price > 0
    assert setup_coin.usd_price > 0
    assert "BTC-BEP20" in [i.coin for i in setup_coin_btc.related_coins]
    assert "BTC-segwit" in [i.coin for i in setup_coin_btc.related_coins]

    assert setup_coin_ltc.is_valid
    assert setup_coin.is_valid
    assert not setup_coin_atom.is_valid
    assert not setup_coin_bad.is_valid


def test_get_gecko_price_and_mcap(
    setup_get_gecko_price_and_mcap_doc, setup_get_gecko_price_and_mcap_ltc
):
    ltc_info = setup_get_gecko_price_and_mcap_ltc
    assert len(ltc_info) == 2
    assert ltc_info[0] < ltc_info[1]
    assert ltc_info[0] > 0
    assert ltc_info[1] > 0

    doc_info = setup_get_gecko_price_and_mcap_doc
    assert len(doc_info) == 2
    assert doc_info[0] == doc_info[1]
    assert doc_info[0] == 0
    assert doc_info[1] == 0
