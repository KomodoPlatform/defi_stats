import pytest

from util.exceptions import BadPairFormatError


def test_BadPairFormatError():
    with pytest.raises(BadPairFormatError):
        raise BadPairFormatError(msg="failed")
