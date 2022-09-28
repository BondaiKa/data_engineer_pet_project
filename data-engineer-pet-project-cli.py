import logging

import click

from cli.jobs.join_citibike_weather_cli import cli as join_citibike_weather_cli
from cli.jobs.load_citibike_dataset_to_parquet_cli import cli as load_bike_dataset_to_parquet_cli
from cli.jobs.load_weather_to_parquet_cli import cli as load_weather_to_parquet_cli
from cli.jobs.make_reports import cli as make_reports
from cli.load.load_citibike_tripdata import cli as load_citibike_dataset_locally_cli

cli = click.CommandCollection(sources=[
    load_citibike_dataset_locally_cli,
    load_bike_dataset_to_parquet_cli,
    join_citibike_weather_cli,
    load_weather_to_parquet_cli,
    make_reports,
])

if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)
    cli()
