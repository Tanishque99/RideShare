SET DATABASE = rideshare;

CREATE TABLE IF NOT EXISTS staging_nyc_raw (
    ride_id        UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    vendor_id      STRING,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance   FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude  FLOAT,
    dropoff_longitude FLOAT,
    dropoff_latitude  FLOAT,
    total_amount    FLOAT
);

CREATE TABLE IF NOT EXISTS nyc_clean (
    ride_id        UUID PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance   FLOAT,
    pickup_lon      FLOAT,
    pickup_lat      FLOAT,
    dropoff_lon     FLOAT,
    dropoff_lat     FLOAT,
    total_amount    FLOAT
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id      UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name           STRING,
    current_lon    FLOAT,
    current_lat    FLOAT,
    status         STRING,  -- 'AVAILABLE','EN_ROUTE','IN_TRANSIT'
    last_updated   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rides_p (
    ride_id        UUID PRIMARY KEY,
    requested_at   TIMESTAMP,
    pickup_lon     FLOAT,
    pickup_lat     FLOAT,
    dropoff_lon    FLOAT,
    dropoff_lat    FLOAT,
    passenger_count INT,
    assigned_driver UUID REFERENCES drivers(driver_id),
    status         STRING,  -- 'REQUESTED','ASSIGNED','COMPLETED'
    created_at     TIMESTAMP DEFAULT now()
);
-- PARTITION BY RANGE (requested_at);

CREATE TABLE IF NOT EXISTS trips_p (
    trip_id        UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ride_id        UUID REFERENCES rides_p(ride_id),
    driver_id      UUID REFERENCES drivers(driver_id),
    start_time     TIMESTAMP,
    end_time       TIMESTAMP,
    total_amount   FLOAT,
    distance       FLOAT
) ;
-- PARTITION BY RANGE (start_time);

-- ALTER TABLE rides_p ADD PARTITION rides_2015_01 VALUES FROM ('2015-01-01') TO ('2015-02-01');
-- ALTER TABLE trips_p ADD PARTITION trips_2015_01 VALUES FROM ('2015-01-01') TO ('2015-02-01');
