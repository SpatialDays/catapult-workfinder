"""Main module."""
import logging
import sys

from workfinder import get_default_s3_api, get_default_nats_api, get_default_redis_api, get_default_esa_api
from workfinder.search.s1 import S1


# Mapping of processor names to objects.
# Note: Names must be upper case.
from workfinder.search.s1_ard import S1ARD

processors = {
    "S1": S1(get_default_s3_api(), get_default_redis_api(), get_default_esa_api()),
    "S1_ARD": S1ARD(get_default_s3_api(), get_default_nats_api())
}


def main():
    if len(sys.argv) > 1:
        param = sys.argv[1]

        limit = -1
        if len(sys.argv)> 2:
            limit = int(sys.argv[2])

        if param:
            param = param.upper()
            if processors[param]:
                logging.info(f"starting work search for {param}")
                work = processors[param].find_new_work()
                if limit > 0:
                    processors[param].submit_tasks(work.head(limit))
                else:
                    processors[param].submit_tasks(work)
                logging.info(f"done work search for {param}")
            else:
                print(f"unknown processor type {param}")
    else:
        print(f"not enough arguments")


if __name__ == '__main__':
    main()
