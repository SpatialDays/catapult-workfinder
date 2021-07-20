import abc
import json
import logging

from libcatapult.queues.base_queue import BaseQueue
from libcatapult.queues.redis import RedisQueue

from workfinder import get_config


class BaseWaiter(object):

    def __init__(self, q : BaseQueue):
        self.q = q

    @abc.abstractmethod
    def check_order(self, order_id: str):
        pass

    @abc.abstractmethod
    def send_complete_order(self, order_details: dict):
        pass

    @abc.abstractmethod
    def get_redis_source_queue(self):
        pass

    def process(self):

        # read all the entries out of the redis queue into a list
        self.q.connect()
        queue_name = self.get_redis_source_queue()

        to_check = []
        while not self.q.empty(queue_name):
            item = self.q.receive(queue_name, timeout=600)
            to_check.append(item)

        logging.info(f"got {len(to_check)} entries to check on.")
        # for each entry check the order
        # if not ready put back in the redis queue
        # if ready send to target
        for item in to_check:
            payload = json.loads(item)
            logging.info(f"checking on {item}")
            if self.check_order(payload['order_id']):
                logging.info(f"{payload['order_id']} completed!")
                self.send_complete_order(payload)
            else:
                logging.info(f"{payload['order_id']} still pending")
                q.publish(queue_name, item)
