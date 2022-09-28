import logging
from datetime import datetime

import click

from data_engineer_pet_project.jobs.join_weather_bike import JoinedWeatherBikeJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
@click.option('--date', '-d', type=click.DateTime(), required=True)
def join_citibike_weather_datasets_cli(date: datetime):
    """Join Weather Citibike datasets

    :param date: last month date
    :return:
    """
    log.info(f"Join weather, citibike datasets for {date:'%Y-%m'}...")
    bike_weather_job = JoinedWeatherBikeJob()
    bike_weather_job.run(date=date)
    log.info(f"`joining datasets cli done...")
