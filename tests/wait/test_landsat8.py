from unittest.mock import MagicMock

from tests.utils import mock_redis
from workfinder import EspaAPI
from workfinder.wait.landsat8 import Landsat8


def mock_espa_api():
    espa = EspaAPI("host", "username", "password")
    espa.call = MagicMock()
    return espa


def test_incomplete():
    redis = mock_redis()
    espa = mock_espa_api()

    espa.call.return_value = {"test_order_id": []}

    landsat = Landsat8(redis, espa)

    assert not landsat.check_order("test_order_id")


def test_complete():
    redis = mock_redis()
    espa = mock_espa_api()

    # TODO: fill this in with real stuff
    espa.call.return_value = {"test_order_id": [{}, {}, {}]}

    landsat = Landsat8(redis, espa)

    assert landsat.check_order("test_order_id")


