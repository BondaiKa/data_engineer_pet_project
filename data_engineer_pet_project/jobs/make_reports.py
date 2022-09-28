from datetime import datetime

from pyspark.sql import DataFrame

from data_engineer_pet_project.base.utils import get_bike_weather_dataset_file_path, \
    get_bike_weather_temperature_report_file_path
from data_engineer_pet_project.config import Config
from data_engineer_pet_project.jobs.base import BaseJob
from data_engineer_pet_project.schema.bike_weather import BikeWeatherSchema
from data_engineer_pet_project.transformers.temperature import get_temperature_trip_dependency


class BikeTripTemperatureDependencyJob(BaseJob):
    """Find dependency on trip according to weather"""

    def get_staging_joined_dataset_parquet_paths(self, date: datetime):
        filename = f"{get_bike_weather_dataset_file_path(date=date)}.parquet"
        return Config().get_hdfs_url + str(Config().get_hdfs_bike_weather_core_path / "staging" / filename)

    def get_bike_weather_temperature_report_path(self, date):
        filename = get_bike_weather_temperature_report_file_path(date=date)
        return Config().get_hdfs_url + str(Config().get_hdfs_dataset_core_path / "public" / f"{filename}.csv")

    def _get_dataset_paths(self, date: datetime):
        return self.get_staging_joined_dataset_parquet_paths(date=date)

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return get_temperature_trip_dependency(df=df,
                                               datetime_col=BikeWeatherSchema.datetime,
                                               temperature=BikeWeatherSchema.temp,
                                               started_at=BikeWeatherSchema.started_at)

    def filter_df(self, dataset: DataFrame) -> DataFrame:
        return dataset

    def save(self, df: DataFrame, date: datetime, *args, **kwargs):
        df.repartition(1).write.mode('overwrite').csv(self.get_bike_weather_temperature_report_path(date),
                                                      header=True,
                                                      sep=',')
