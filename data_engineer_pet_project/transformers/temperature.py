from pyspark.sql import DataFrame, functions as f


def get_temperature_trip_dependency(df: DataFrame, datetime_col: str,
                                    temperature: str, started_at: str, numb_of_trip: str = 'numb_of_trip',
                                    temp_group: str = 'temp_group'):
    """Find number of trips in different temperature group

    Find number of trips in less than 10°C, between 10°C and 20°C and more than 20°C groups

    :param df: joined citibike, weather dataset
    :param datetime_col: temperature measurement time
    :param temperature: temperature in °C
    :param started_at: started at bike trip time
    :param numb_of_trip: number of trips column name
    :param temp_group: column group column name

    :return:
    """
    return df.select(f.col(datetime_col), f.col(temperature), f.col(started_at)) \
        .groupby(f.to_date(f.col(datetime_col), format='yyyy-MM-dd').alias(datetime_col)) \
        .agg(f.count(started_at).alias(numb_of_trip), f.avg(f.col(temperature)).alias(temperature)) \
        .select(f.when(f.col(temperature) <= 10, 0) \
                .when((f.col(temperature) < 20) & (f.col(temperature) > 10), 1) \
                .otherwise(2).alias(temp_group),
                f.col(numb_of_trip)
                ) \
        .groupby(temp_group) \
        .agg(f.sum(numb_of_trip).alias(numb_of_trip)) \
        .orderBy(temp_group)
