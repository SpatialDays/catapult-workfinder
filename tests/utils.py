
from unittest.mock import MagicMock

from libcatapult.queues.redis import RedisQueue


def mock_redis():
    mock_queue = RedisQueue("a host", "12345")

    mock_queue.connect = MagicMock()
    mock_queue.publish = MagicMock()
    return mock_queue
