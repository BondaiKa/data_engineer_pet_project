from datetime import datetime

from pyspark.sql import DataFrame

from data_engineer_pet_project.base.utils import BIKE_DATASET_CSV_FILE_EXTENSION, BIKE_DATASET_FILE_PATTERN, \
    BIKE_DATASET_PARQUET_FILE_EXTENSION
from data_engineer_pet_project.config import Config
from data_engineer_pet_project.jobs.base import BaseJob
from data_engineer_pet_project.jobs.session import Session


class BikeDatasetLandingJob(BaseJob):
    """Clean and convert bike dataset to parquet"""

    def get_landing_bike_dataset_csv_paths(self, date: datetime) -> str:
        filename = f"{date.year}{date.month:02d}-{BIKE_DATASET_FILE_PATTERN}{BIKE_DATASET_CSV_FILE_EXTENSION}"
        return Config().get_hdfs_url + str(
            Config().get_hdfs_dataset_core_path / 'landing' / Config().get_hdfs_bike_dataset_name / filename)

    def _get_dataset_paths(self, date: datetime) -> str:
        return self.get_landing_bike_dataset_csv_paths(date=date)

    def get_landing_bike_dataset_parquet_paths(self, date: datetime) -> str:
        filename = f"{date.year}{date.month:02d}-{BIKE_DATASET_FILE_PATTERN}{BIKE_DATASET_PARQUET_FILE_EXTENSION}"
        return Config().get_hdfs_url + str(
            Config().get_hdfs_dataset_core_path / 'landing' / Config().get_hdfs_bike_dataset_name / filename)

    def extract(self, date: datetime) -> DataFrame:
        return self.filter_df(dataset=Session().load_csv_file(paths=[str(self._get_dataset_paths(date))], header=True))

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df

    def filter_df(self, dataset: DataFrame) -> DataFrame:
        return dataset

    def save(self, df: DataFrame, date: datetime):
        df.repartition(1).write.mode('overwrite').parquet(self.get_landing_bike_dataset_parquet_paths(date))
