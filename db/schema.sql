-- Set active DB
SET DATABASE = rideshare;

-- ===============================
--  1. RAW STAGING TABLE
-- ===============================
CREATE TABLE IF NOT EXISTS staging_nyc_raw (
    ride_id           UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    vendor_id         STRING,
    pickup_datetime   TIMESTAMP,
    dropoff_datetime  TIMESTAMP,
    passenger_count   INT,
    trip_distance     FLOAT,
    pickup_longitude  FLOAT,
    pickup_latitude   FLOAT,
    dropoff_longitude FLOAT,
    dropoff_latitude  FLOAT,
    total_amount      FLOAT
);

-- ===============================
--  2. CLEANED TABLE
-- ===============================
CREATE TABLE IF NOT EXISTS nyc_clean (
    ride_id         UUID PRIMARY KEY,
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

-- ===============================
--  3. DRIVERS TABLE
-- ===============================
CREATE TABLE IF NOT EXISTS drivers (
    driver_id      UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name           STRING,
    current_lon    FLOAT,
    current_lat    FLOAT,
    status         STRING DEFAULT 'AVAILABLE',   -- Added safe default
    last_updated   TIMESTAMP DEFAULT NOW()
);

-- ===============================
--  4. RIDES TABLE (PARTITIONS READY)
-- ===============================
CREATE TABLE IF NOT EXISTS rides_p (
    ride_id        UUID PRIMARY KEY,
    requested_at   TIMESTAMP,
    pickup_lon     FLOAT,
    pickup_lat     FLOAT,
    dropoff_lon    FLOAT,
    dropoff_lat    FLOAT,
    passenger_count INT,
    assigned_driver UUID REFERENCES drivers(driver_id),
    status         STRING DEFAULT 'REQUESTED',   -- Added safe default
    created_at     TIMESTAMP DEFAULT NOW()
);

-- ADD PARTITION HELPER COLUMN (NEW)
ALTER TABLE rides_p 
ADD COLUMN IF NOT EXISTS partition_date DATE GENERATED ALWAYS AS (requested_at::DATE) STORED;


-- ===============================
--  5. TRIPS TABLE (WITH CONSTRAINTS & DURATION)
-- ===============================
CREATE TABLE IF NOT EXISTS trips_p (
    trip_id        UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ride_id        UUID REFERENCES rides_p(ride_id) ON DELETE CASCADE,
    driver_id      UUID REFERENCES drivers(driver_id) ON DELETE SET NULL,
    start_time     TIMESTAMP,
    end_time       TIMESTAMP,
    total_amount   FLOAT,
    distance       FLOAT
);

-- Ensure 1 trip per ride
ALTER TABLE trips_p
ADD CONSTRAINT IF NOT EXISTS unique_trip_per_ride UNIQUE (ride_id);

-- Add partition helper column
ALTER TABLE trips_p 
ADD COLUMN IF NOT EXISTS partition_date DATE GENERATED ALWAYS AS (start_time::DATE) STORED;

-- Add computed duration
ALTER TABLE trips_p 
ADD COLUMN IF NOT EXISTS duration_seconds FLOAT GENERATED ALWAYS AS (
    EXTRACT(EPOCH FROM (end_time - start_time))
) STORED;

-- Add data safety constraints
ALTER TABLE trips_p
ADD CONSTRAINT IF NOT EXISTS chk_distance CHECK (distance >= 0);

ALTER TABLE trips_p
ADD CONSTRAINT IF NOT EXISTS chk_fare CHECK (total_amount >= 0);

ALTER TABLE rides_p
ADD COLUMN IF NOT EXISTS assigned_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS rejection_reason STRING;

ALTER TABLE rides_p ADD COLUMN IF NOT EXISTS status STRING DEFAULT 'REQUESTED';

ALTER TABLE rides_p ADD COLUMN IF NOT EXISTS retries INT DEFAULT 0;
ALTER TABLE rides_p ADD COLUMN IF NOT EXISTS match_latency_ms FLOAT;

ALTER TABLE rides_p
ADD CONSTRAINT unique_active_driver_per_ride
UNIQUE (assigned_driver) 
WHERE status IN ('ASSIGNED', 'EN_ROUTE');

