from pyspark.sql import DataFrame, functions as f


def get_weather_required_fields(df: DataFrame, name: str, datetime_col: str,
                                temperature: str, precipitation: str,
                                snow: str, wind_speed: str):
    """Get needed for research weather fields

    :param df: weather dataframe
    :param name: city name
    :param datetime: date
    :param temperature: weather temperature
    :param precipitation: weather precipitation
    :param snow: is snow or not
    :param wind_speed: wind speed
    :return: desired weather dataframe
    """
    return df.select(f.col(name),
                     f.to_timestamp(f.col(datetime_col), format="yyyy-MM-dd'T'HH:mm:ss").alias(datetime_col),
                     f.col(temperature).cast("double"),
                     f.col(precipitation).cast("double"), f.col(snow).cast("int"),
                     f.col(wind_speed).cast("double"))
