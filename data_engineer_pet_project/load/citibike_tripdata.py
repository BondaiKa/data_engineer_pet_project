import logging
from datetime import datetime

import boto3
from botocore import UNSIGNED
from botocore.client import Config as BotoConfig
from hdfs import InsecureClient

from data_engineer_pet_project.base.utils import BIKE_DATASET_FILE_PATTERN
from data_engineer_pet_project.config import Config

log = logging.getLogger(__name__)


def load_citibike_dataset_locally(date: datetime) -> None:
    """Load citibike dataset from s3 bucket locally

    Dataset: https://ride.citibikenyc.com/system-data
    """
    log.info(f'Start to load citibike dataset from s3 bucket for {date:%Y-%m} locally...')
    s3 = boto3.client('s3', config=BotoConfig(signature_version=UNSIGNED))
    bucket = Config().get_bike_dataset_bucket_name

    filename = f"{date.year}{date.month:02d}-{BIKE_DATASET_FILE_PATTERN}.csv.zip"
    full_path = Config().get_bike_dataset_local_core_path / filename

    with open(full_path, 'wb') as f:
        s3.download_fileobj(bucket, filename, f)

    log.info('Dataset has been downloaded...')


def load_citibike_dataset_to_hdfs(date: datetime) -> None:
    """Load from s3 bucket to hdfs


    Dataset: https://ride.citibikenyc.com/system-data
    """
    # TODO: @Karim fix hadoop hdfs configs.
    log.info(f'Start to load citibike dataset from s3 bucket for {date:%Y-%m} to hdfs...')
    s3 = boto3.client('s3', config=BotoConfig(signature_version=UNSIGNED))
    bucket = Config().get_bike_dataset_bucket_name
    client_hdfs = InsecureClient(Config().get_hdfs_url, user=Config().get_hadoop_user)

    filename = f"{date.year}{date.month:02d}-{BIKE_DATASET_FILE_PATTERN}{BIKE_DATASET_ZIP_FILE_EXTENSION}"
    hdfs_bike_core_path = Config().get_hdfs_bike_core_path

    with client_hdfs.write(hdfs_bike_core_path / filename) as writer:
        s3.download_fileobj(bucket, filename, writer)

    log.info('Dataset has been downloaded...')


if __name__ == '__main__':
    log.info('Start to download citibike dataset...')
    load_citibike_dataset_to_hdfs(date=datetime(year=2022, month=4, day=1))
    log.info('File downloaded...')
