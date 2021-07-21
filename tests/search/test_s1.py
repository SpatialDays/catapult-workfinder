from unittest.mock import MagicMock

import pandas as pd
from libcatapult.queues.redis import RedisQueue

from tests.utils import mock_redis
from workfinder.search.s1 import S1


def test_s1_send_none():

    mock_queue = mock_redis()

    s1 = S1(None, mock_queue, None)

    s1.submit_tasks(None)

    mock_queue.publish.assert_not_called()


def test_s1_send():
    mock_queue = mock_redis()

    s1 = S1(None, mock_queue, None)

    inputs = pd.DataFrame({'id': ["abc"], 'url': ["https://abc.com/abc"], 'hemisphere': ["e"]})

    s1.submit_tasks(inputs)

    mock_queue.publish.assert_called_once()
