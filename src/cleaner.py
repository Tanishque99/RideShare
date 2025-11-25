# src/cleaner.py
from db import get_cursor

def clean_data():
    """
    Clean data from staging_nyc_raw into nyc_clean by applying
    simple quality and geographic filters.
    """
    
    sql = """
    INSERT INTO nyc_clean (
      ride_id,
      pickup_datetime,
      dropoff_datetime,
      passenger_count,
      trip_distance,
      pickup_lon,
      pickup_lat,
      dropoff_lon,
      dropoff_lat,
      total_amount
    )
    SELECT
      ride_id,
      pickup_datetime,
      dropoff_datetime,
      passenger_count,
      trip_distance,
      pickup_longitude,
      pickup_latitude,
      dropoff_longitude,
      dropoff_latitude,
      total_amount
    FROM staging_nyc_raw
    WHERE trip_distance > 0.5
      AND total_amount > 3.0
      AND pickup_longitude BETWEEN -74.3 AND -73.5
      AND pickup_latitude  BETWEEN 40.3 AND 41.0
    ON CONFLICT (ride_id) DO NOTHING;
    """
    with get_cursor(commit=True) as cur:
        print("[cleaner] Cleaning data from staging_nyc_raw into nyc_clean...")
        cur.execute(sql)
        print("[cleaner] Done.")


if __name__ == "__main__":
    clean_data()
