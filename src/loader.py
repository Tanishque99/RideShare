# src/loader.py
from db import get_cursor, run_txn

def load_synthetic(n_rows=100):
    # Build SQL with generate_series for N rows
    sql = f"""
    INSERT INTO staging_nyc_raw (
      vendor_id,
      pickup_datetime,
      dropoff_datetime,
      passenger_count,
      trip_distance,
      pickup_longitude,
      pickup_latitude,
      dropoff_longitude,
      dropoff_latitude,
      total_amount
    )
    SELECT
      'V' || (1 + floor(random()*3)::int)                                       AS vendor_id,
      ts                                                                        AS pickup_datetime,
      ts + (interval '5 minutes' + random() * interval '45 minutes')           AS dropoff_datetime,
      1 + floor(random()*4)::int                                               AS passenger_count,
      dist                                                                      AS trip_distance,
      pickup_lon,
      pickup_lat,
      pickup_lon + (random() - 0.5) * 0.05                                      AS dropoff_lon,
      pickup_lat + (random() - 0.5) * 0.05                                      AS dropoff_lat,
      2.5 + dist * (1.5 + random()*1.0) + random()*3.0                          AS total_amount
    FROM (
      SELECT
        '2015-01-01'::timestamp + (random() * interval '31 days')               AS ts,
        -74.05 + random()*0.3                                                   AS pickup_lon,
        40.63 + random()*0.22                                                   AS pickup_lat,
        0.5 + random()*19.5                                                     AS dist
      FROM generate_series(1, {n_rows})
    ) AS s;
    """
    with get_cursor(commit=True) as cur:
        print(f"Inserting {n_rows} synthetic rows into staging_nyc_raw...")
        cur.execute(sql)
        print("Done.")

if __name__ == "__main__":
    load_synthetic(100)  # adjust N as needed
