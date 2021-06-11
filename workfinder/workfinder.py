"""Main module."""
import logging
import sys

from workfinder.search.s1 import S1

processors = {
    "S1": S1()
}


def main():
    if len(sys.argv) > 1:
        param = sys.argv[1]
        if param:
            param = param.upper()
            if processors[param]:
                logging.info(f"starting work search for {param}")
                processors[param].find_new_work()
                logging.info(f"done work search for {param}")
            else:
                print(f"unknown processor type {param}")
    else:
        print(f"not enough arguments")


if __name__ == '__main__':
    main()
