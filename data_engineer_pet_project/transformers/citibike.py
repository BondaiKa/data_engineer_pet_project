from pyspark.sql import DataFrame, functions as f


def get_bike_required_fields(df: DataFrame, rideable_type: str, started_at: str, ended_at: str, start_lat: str,
                             start_lng: str, end_lat: str, end_lng: str, member_casual: str):
    """Get only necessary citibike fields with their type"""
    return df.select(
        f.col(rideable_type),
        f.to_timestamp(f.col(started_at)).alias(started_at), f.to_timestamp(f.col(ended_at)).alias(ended_at),
        f.col(start_lat).cast("double"), f.col(start_lng).cast("double"),
        f.col(end_lat).cast("double"), f.col(end_lng).cast("double"),
        f.col(member_casual)
    )
