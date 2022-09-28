import calendar
from datetime import datetime

from pyspark.sql import DataFrame

from data_engineer_pet_project.base.utils import get_weather_dataset_file_path
from data_engineer_pet_project.config import Config
from data_engineer_pet_project.jobs import BaseJob
from data_engineer_pet_project.jobs.session import Session


class WeatherDatasetLandingJob(BaseJob):
    """Load and transform weather dataset to hdfs job

    dataset info: https://www.visualcrossing.com/weather/weather-data-services
    """

    def get_landing_weather_dataset_csv_paths(self, start_date: datetime, end_date: datetime) -> str:
        """Get initial weather dataset"""
        filename = get_weather_dataset_file_path(start_date=start_date,
                                                 end_date=end_date)
        return Config().get_hdfs_url + str(
            Config().get_hdfs_dataset_core_path / 'landing' / Config().get_hdfs_weather_dataset_name /
            f"{filename}.csv")

    def _get_dataset_paths(self, date: datetime) -> str:
        return self.get_landing_weather_dataset_csv_paths(
            start_date=datetime(
                year=date.year,
                month=date.month,
                day=1,
            ),
            end_date=datetime(
                year=date.year,
                month=date.month,
                day=calendar.monthrange(date.year, date.month)[1]
            )
        )

    def get_landing_weather_dataset_parquet_paths(self, date: datetime) -> str:
        """Get weather dataset with parquet format"""
        filename = get_weather_dataset_file_path(
            start_date=datetime(
                year=date.year,
                month=date.month,
                day=1,
            ),
            end_date=datetime(
                year=date.year,
                month=date.month,
                day=calendar.monthrange(date.year, date.month)[1]
            )
        )
        return Config().get_hdfs_url + str(
            Config().get_hdfs_dataset_core_path / 'landing' / Config().get_hdfs_weather_dataset_name /
            f"{filename}.parquet")

    def extract(self, date: datetime) -> DataFrame:
        return self.filter_df(dataset=Session().load_csv_file(paths=[str(self._get_dataset_paths(date))], header=True))

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df

    def filter_df(self, dataset: DataFrame) -> DataFrame:
        return dataset

    def save(self, df: DataFrame, date: datetime) -> None:
        df.repartition(1).write.mode('overwrite').parquet(self.get_landing_weather_dataset_parquet_paths(date))
