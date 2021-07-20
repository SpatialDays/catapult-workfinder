import logging

import click

from workfinder import default_redis_api, default_espa_api
from workfinder.wait.landsat8 import Landsat8

processors = {
    "LANDSAT8": Landsat8(default_redis_api(), default_espa_api())
}


@click.command()
@click.argument("process")
def main(process):
    param = process.upper()
    if param in processors:
        logging.info(f"starting completed order search for {param}")
        processors[param].process()
    else:
        print(f"unknown processor type {param}")


if __name__ == '__main__':
    main()
