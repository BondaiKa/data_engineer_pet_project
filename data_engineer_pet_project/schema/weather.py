class WeatherVisualCrossingOriginSchema:
    """Original weather dataset schema"""
    name = 'name'
    datetime = 'datetime'
    temp = 'temp'
    feels_like = 'feelslike'
    dew = 'dew'
    humidity = 'humidity'
    precip = 'precip'
    precip_prob = 'precipprob'
    precip_type = 'preciptype'
    snow = 'snow'
    snow_depth = 'snowdepth'
    wind_gust = 'windgust'
    wind_speed = 'windspeed'
    wind_dir = 'winddir'
    sea_level_pressure = 'sealevelpressure'
    cloud_cover = 'cloudcover'
    visibility = 'visibility'
    solar_radiation = 'solarradiation'
    uv_index = 'uvindex'
    severerisk = 'severerisk'
    conditions = 'conditions'
    icon = 'icon'
    stations = 'stations'


class WeatherVisualCrossingShortSchema:
    """Weather dataset schema with necessary fields"""
    name = WeatherVisualCrossingOriginSchema.name
    datetime = WeatherVisualCrossingOriginSchema.datetime
    temp = WeatherVisualCrossingOriginSchema.temp
    precip = WeatherVisualCrossingOriginSchema.precip
    snow = WeatherVisualCrossingOriginSchema.snow
    wind_speed = WeatherVisualCrossingOriginSchema.wind_speed
