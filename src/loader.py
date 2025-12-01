# src/loader.py
from db import get_cursor

def load_synthetic(n_rows=100, clear_existing=False):
    """
    Generates synthetic ride data over a larger area + realistic time distribution
    to avoid clustering and reduce CockroachDB contention.
    
    Parameters:
        n_rows (int): number of synthetic rides to insert
        clear_existing (bool): if True → wipe staging before new load
    """
    print(f"[load_synthetic] 🚕 Generating {n_rows} rides")

    if clear_existing:
        with get_cursor(commit=True) as cur:
            print("[load_synthetic] 🧹 Clearing existing synthetic data...")
            cur.execute("DELETE FROM staging_nyc_raw;")

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
        'V' || (1 + floor(random()*3)::int) AS vendor_id,
        ts AS pickup_datetime,
        ts + (interval '5 minutes' + random() * interval '45 minutes') AS dropoff_datetime,
        1 + floor(random()*4)::int AS passenger_count,
        dist AS trip_distance,
        pickup_lon,
        pickup_lat,
        pickup_lon + (random() - 0.5) * 0.15 AS dropoff_lon,
        pickup_lat + (random() - 0.5) * 0.15 AS dropoff_lat,
        2.5 + dist * (1.5 + random()*2.0) + random()*5.0 AS total_amount
    FROM (
        SELECT
            CASE
                WHEN random() < 0.5 THEN
                    '2025-11-29 08:00'::timestamp  -- Morning rush
                    + (row_number() OVER () * interval '10 seconds')
                    + (random() * interval '20 minutes')
                ELSE
                    '2025-11-29 18:00'::timestamp  -- Evening peak
                    + (row_number() OVER () * interval '10 seconds')
                    + (random() * interval '20 minutes')
            END AS ts,

            -74.25 + random() * 0.5 AS pickup_lon,
            40.40 + random() * 0.4 AS pickup_lat,
            1.0 + random() * 49.0 AS dist  -- 🚕 Trip up to 50 km
        FROM generate_series(1, {n_rows})
    ) AS synthetic;
    """

    with get_cursor(commit=True) as cur:
        print(f"[load_synthetic] 🚀 Inserting {n_rows} synthetic rows...")
        cur.execute(sql)
        print(f"[load_synthetic] ✔ Complete")


if __name__ == "__main__":
    # Safe for large-scale load tests
    load_synthetic(1000, clear_existing=True)

