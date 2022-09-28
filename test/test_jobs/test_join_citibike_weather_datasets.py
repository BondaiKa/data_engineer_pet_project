import pytest
from pyspark.sql import Row

from data_engineer_pet_project.transformers.join_bike_weather_datasets import join_weather_bike_datasets_job


@pytest.fixture()
def weather_dataframe(spark_session):
    # TODO:
    return spark_session.createDataFrame([
        Row(),
    ])


@pytest.fixture()
def bike_dataframe(spark_session):
    # TODO:
    return spark_session.createDataFrame(
        Row(),
    )


@pytest.fixture()
def expected_joined_dataframe(spark_session):
    # TODO:
    return [
        (),
        (),
        (),
    ]


def test_transform_join_citibike_weather_dataframes(weather_dataframe, bike_dataframe, expected_joined_dataframe):
    df = join_weather_bike_datasets_job(weather_df=weather_dataframe, bike_df=bike_dataframe)
    assert sorted(
        [() for row in df.collect()],
        key=lambda x: (x[0], x[1])) == expected_joined_dataframe
