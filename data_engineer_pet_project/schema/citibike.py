class NewCitibikeOriginalSchema:
    ride_id = 'ride_id'
    rideable_type = 'rideable_type'
    started_at = 'started_at'
    ended_at = 'ended_at'
    start_station_name = 'start_station_name'
    start_station_id = 'start_station_id'
    end_station_name = 'end_station_name'
    end_station_id = 'end_station_id'
    start_lat = 'start_lat'
    start_lng = 'start_lng'
    end_lat = 'end_lat'
    end_lng = 'member_casual'
    member_casual = 'member_casual'


class NewCitibikeShortSchema:
    rideable_type = NewCitibikeOriginalSchema.rideable_type
    started_at = NewCitibikeOriginalSchema.started_at
    ended_at = NewCitibikeOriginalSchema.ended_at
    start_lat = NewCitibikeOriginalSchema.start_lat
    start_lng = NewCitibikeOriginalSchema.start_lng
    end_lat = NewCitibikeOriginalSchema.end_lat
    end_lng = NewCitibikeOriginalSchema.end_lng
    member_casual = NewCitibikeOriginalSchema.member_casual
