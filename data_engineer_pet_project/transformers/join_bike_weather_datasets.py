from pyspark.sql import DataFrame, functions as f


def join_weather_bike_datasets_job(
        weather_df: DataFrame, bike_df: DataFrame,
        # weather_name: str, weather_datetime: str, weather_temp: str,
        # weather_precip: str, weather_snow: str, weather_wind_speed: str,
        # bike_rideable_type: str, bike_started_at: str, bike_ended_at,
        # bike_start_lat, bike_start_lng, bike_end_lat, bike_end_lng, bike_member_casual: str,
    ):
    """Join weather and bike trip dataset"""
    return bike_df.join(f.broadcast(weather_df), bike_df.started_at == weather_df.datetime, how="inner")
