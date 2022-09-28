from pyspark.sql import DataFrame, functions as f


def join_weather_bike_datasets_job(weather_df: DataFrame, bike_df: DataFrame):
    """Join weather and bike trip dataset

    :param weather_df:  weather dataframe
    :param bike_df: citibike dataframe
    :return: joined dataset
    """
    return bike_df.join(
        f.broadcast(weather_df),
        f.to_date(bike_df.started_at, format='yyyy-MM-dd HH') == f.to_date(weather_df.datetime, format='yyyy-MM-dd HH'),
        how="inner"
    )
