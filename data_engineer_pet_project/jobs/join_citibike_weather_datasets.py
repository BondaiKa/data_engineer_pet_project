import logging
from datetime import datetime
from typing import Tuple

from pyspark.sql import DataFrame

from data_engineer_pet_project.datalake.staging import BaseStagingArea
from data_engineer_pet_project.jobs import BaseJob
from data_engineer_pet_project.jobs.session import Session
from data_engineer_pet_project.schema.citibike import NewCitibikeShortSchema
from data_engineer_pet_project.schema.weather import WeatherVisualCrossingShortSchema
from data_engineer_pet_project.transformers.citibike import get_bike_required_fields
from data_engineer_pet_project.transformers.join_bike_weather_datasets import join_weather_bike_datasets_job
from data_engineer_pet_project.transformers.weather import get_weather_required_fields

log = logging.getLogger(__name__)


class JoinedWeatherBikeJob(BaseJob):
    """Join and save weather bike dataset"""
    area = BaseStagingArea()

    # def get_weather_dataset_paths(self, date: datetime) -> str:
    #     return WeatherDatasetLandingJob().get_landing_weather_dataset_parquet_paths(date=date)
    #
    # def get_bike_dataset_paths(self, date: datetime) -> str:
    #     return BikeDatasetLandingJob().get_landing_bike_dataset_parquet_paths(date=date)

    def extract(self, date: datetime) -> Tuple[DataFrame, DataFrame]:
        """Load dataset"""
        weather_df = self.filter_df(Session().load_dataframe(paths=self.area.get_landing_weather_dataset_paths(date)))
        bike_df = self.filter_df(Session().load_dataframe(paths=self.area.get_landing_bike_dataset_paths(date)))
        return weather_df, bike_df

    # def get_staging_joined_dataset_parquet_paths(self, date: datetime):
    #     filename = f"{get_bike_weather_dataset_file_path(date=date)}.parquet"
    #     return Config().get_hdfs_url + str(Config().get_hdfs_bike_weather_dataset_name / "staging" / filename)

    def transform(self, weather_df: DataFrame, bike_df: DataFrame, *args, **kwargs) -> DataFrame:
        return join_weather_bike_datasets_job(
            weather_df=get_weather_required_fields(
                df=weather_df,
                name=WeatherVisualCrossingShortSchema.name,
                datetime_col=WeatherVisualCrossingShortSchema.datetime,
                temperature=WeatherVisualCrossingShortSchema.temp,
                precipitation=WeatherVisualCrossingShortSchema.precip,
                snow=WeatherVisualCrossingShortSchema.snow,
                wind_speed=WeatherVisualCrossingShortSchema.wind_speed

            ),
            bike_df=get_bike_required_fields(
                df=bike_df,
                rideable_type=NewCitibikeShortSchema.rideable_type,
                started_at=NewCitibikeShortSchema.started_at,
                ended_at=NewCitibikeShortSchema.ended_at,
                start_lat=NewCitibikeShortSchema.start_lat,
                start_lng=NewCitibikeShortSchema.start_lng,
                end_lat=NewCitibikeShortSchema.end_lat,
                end_lng=NewCitibikeShortSchema.end_lng,
                member_casual=NewCitibikeShortSchema.member_casual)

        )

    def save(self, df: DataFrame, date: datetime, *args, **kwargs):
        for path in self.area.get_staging_joined_dataset_parquet_paths(date=date):
            df.repartition(1).write.mode('overwrite').parquet(path)

    def filter_df(self, dataset: DataFrame) -> DataFrame:
        return dataset

    def run(self, date: datetime):
        log.info(f'Start to extract data for {date}...')
        weather_df, bike_df = self.extract(date)

        log.info(f'Start to join citibike and weather dataframes for {date}...')
        df = self.transform(weather_df=weather_df, bike_df=bike_df)
        self.save(df, date)
