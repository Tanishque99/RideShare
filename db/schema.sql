-- ========================================
-- SET ACTIVE DATABASE
-- ========================================
SET DATABASE = rideshare;

-- ========================================
-- 1️⃣ RAW STAGING TABLE
-- ========================================
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

-- ========================================
-- 2️⃣ CLEANED TABLE
-- ========================================
CREATE TABLE IF NOT EXISTS nyc_clean (
    ride_id          UUID PRIMARY KEY,
    pickup_datetime  TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count  INT,
    trip_distance    FLOAT,
    pickup_lon       FLOAT,
    pickup_lat       FLOAT,
    dropoff_lon      FLOAT,
    dropoff_lat      FLOAT,
    total_amount     FLOAT
);

-- ========================================
-- 3️⃣ DRIVERS TABLE
-- ========================================
CREATE TABLE IF NOT EXISTS drivers (
    driver_id     UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name          STRING,
    current_lon   FLOAT,
    current_lat   FLOAT,
    status        STRING DEFAULT 'AVAILABLE',
    last_updated  TIMESTAMP DEFAULT NOW()
);

-- 🔹 Performance Indexes
CREATE INDEX IF NOT EXISTS idx_drivers_status ON drivers(status);
CREATE INDEX IF NOT EXISTS idx_drivers_last_updated ON drivers(last_updated);

-- ========================================
-- 4️⃣ RIDES TABLE
-- ========================================
CREATE TABLE IF NOT EXISTS rides_p (
    ride_id         UUID PRIMARY KEY,
    requested_at    TIMESTAMP,
    pickup_lon      FLOAT,
    pickup_lat      FLOAT,
    dropoff_lon     FLOAT,
    dropoff_lat     FLOAT,
    passenger_count INT,
    assigned_driver UUID REFERENCES drivers(driver_id),
    status          STRING DEFAULT 'REQUESTED',
    created_at      TIMESTAMP DEFAULT NOW(),

    -- Additional fields
    assigned_at     TIMESTAMP,
    retries         INT DEFAULT 0,
    rejection_reason STRING,
    match_latency_ms FLOAT
);

-- Add partition helper
ALTER TABLE rides_p 
ADD COLUMN IF NOT EXISTS partition_date DATE GENERATED ALWAYS AS (requested_at::DATE) STORED;

-- 🔹 Indexes
CREATE INDEX IF NOT EXISTS idx_rides_status ON rides_p(status);
CREATE INDEX IF NOT EXISTS idx_rides_requested_at ON rides_p(requested_at);
CREATE INDEX IF NOT EXISTS idx_rides_partition ON rides_p(partition_date);

-- Ensure one driver cannot be EN_ROUTE for 2 active rides
ALTER TABLE rides_p
ADD CONSTRAINT IF NOT EXISTS unique_active_driver_per_ride
UNIQUE (assigned_driver)
WHERE status IN ('ASSIGNED', 'EN_ROUTE');

-- ========================================
-- 5️⃣ TRIPS TABLE
-- ========================================
CREATE TABLE IF NOT EXISTS trips_p (
    trip_id       UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ride_id       UUID REFERENCES rides_p(ride_id) ON DELETE CASCADE,
    driver_id     UUID REFERENCES drivers(driver_id) ON DELETE SET NULL,
    start_time    TIMESTAMP,
    end_time      TIMESTAMP,
    total_amount  FLOAT,
    distance      FLOAT
);

ALTER TABLE trips_p
ADD CONSTRAINT IF NOT EXISTS unique_trip_per_ride UNIQUE (ride_id);

ALTER TABLE trips_p
ADD COLUMN IF NOT EXISTS partition_date DATE GENERATED ALWAYS AS (start_time::DATE) STORED;

ALTER TABLE trips_p
ADD COLUMN IF NOT EXISTS duration_seconds FLOAT GENERATED ALWAYS AS (
    EXTRACT(EPOCH FROM (end_time - start_time))
) STORED;

ALTER TABLE trips_p
ADD CONSTRAINT IF NOT EXISTS chk_distance CHECK (distance >= 0);

ALTER TABLE trips_p
ADD CONSTRAINT IF NOT EXISTS chk_fare CHECK (total_amount >= 0);

-- 🔹 Indexes
CREATE INDEX IF NOT EXISTS idx_trips_partition ON trips_p(partition_date);
CREATE INDEX IF NOT EXISTS idx_trips_driver_id ON trips_p(driver_id);

-- ========================================
-- 6️⃣ FINAL OPTIMIZATION INDEXES
-- ========================================
-- Helps batching in match_ride
CREATE INDEX IF NOT EXISTS idx_drivers_available ON drivers(status) WHERE status = 'AVAILABLE';

-- Helps UI deep analysis
CREATE INDEX IF NOT EXISTS idx_rides_latency ON rides_p(match_latency_ms);

CREATE INDEX IF NOT EXISTS idx_rides_retry ON rides_p(retries);
