from data_engineer_pet_project.schema.weather import WeatherVisualCrossingShortSchema
from data_engineer_pet_project.schema.citibike import NewCitibikeShortSchema

class BikeWeatherSchema:
    rideable_type = NewCitibikeShortSchema.rideable_type
    started_at = NewCitibikeShortSchema.started_at
    ended_at = NewCitibikeShortSchema.ended_at
    start_lat = NewCitibikeShortSchema.start_lat
    start_lng = NewCitibikeShortSchema.start_lng
    end_lat = NewCitibikeShortSchema.end_lat
    end_lng = NewCitibikeShortSchema.end_lng
    member_casual = NewCitibikeShortSchema.member_casual
    datetime = WeatherVisualCrossingShortSchema.datetime
    temp = WeatherVisualCrossingShortSchema.temp
    precip = WeatherVisualCrossingShortSchema.precip
    snow = WeatherVisualCrossingShortSchema.snow
    wind_speed = WeatherVisualCrossingShortSchema.wind_speed
