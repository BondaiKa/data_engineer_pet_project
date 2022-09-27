from pyspark.sql import DataFrame, functions as f


def get_temperature_trip_dependency(df: DataFrame, datetime_col: str,
                                    temperature: str, started_at: str):
    """

    :param df:
    :param datetime:
    :param temperature:
    :param started_at:
    :return:
    """
    return df.select(f.col(datetime_col), f.col(temperature), f.col(started_at)) \
        .groupby(f.to_date(f.col(datetime_col), format='yyyy-MM-dd')) \
        .agg(f.count(started_at).alias('number_of_trip'), f.avg(f.col(temperature)))
