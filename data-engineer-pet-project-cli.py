import logging

import click

from cli.load.load_citibike_tripdata import cli as load_citibike_dataset_locally_cli

cli = click.CommandCollection(sources=[
    load_citibike_dataset_locally_cli,
])

if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)
    cli()
