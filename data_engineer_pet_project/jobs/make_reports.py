from datetime import datetime

from pyspark.sql import DataFrame

from data_engineer_pet_project.datalake.public import BasePublicArea
from data_engineer_pet_project.jobs.base import BaseJob
from data_engineer_pet_project.schema.bike_weather import BikeWeatherSchema
from data_engineer_pet_project.transformers.temperature import get_temperature_trip_dependency


class BikeTripTemperatureDependencyJob(BaseJob):
    """Find dependency on trip according to weather"""
    area = BasePublicArea()

    def _get_dataset_paths(self, date: datetime):
        return self.area.get_staging_joined_dataset_parquet_paths(date=date)

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return get_temperature_trip_dependency(df=df,
                                               datetime_col=BikeWeatherSchema.datetime,
                                               temperature=BikeWeatherSchema.temp,
                                               started_at=BikeWeatherSchema.started_at)

    def filter_df(self, dataset: DataFrame) -> DataFrame:
        return dataset

    def save(self, df: DataFrame, date: datetime, *args, **kwargs):
        for path in self.area.get_public_bike_weather_temperature_report_path(date):
            df.repartition(1).write.mode('overwrite').csv(path, header=True, sep=',')
