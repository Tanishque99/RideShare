# src/matcher.py
import math
import time
import random
from datetime import timedelta
from db import run_txn, get_cursor
from geo import get_region  # NEW
 
# GLOBAL ride ID used by run_txn() to record retries
current_retry_ride_id = None


EARTH_R = 6371.0
MAX_NEAREST_DRIVERS = 5

def _haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    return EARTH_R * (2 * math.asin(math.sqrt(
        math.sin((lat2 - lat1) / 2) ** 2 +
        math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )))

def calculate_fare(distance):
    return round(3.0 + distance * 1.8, 2)

def match_ride(ride: dict):
    """
    Try to match a ride to an AVAILABLE driver.

    Returns:
        driver_id on success, or None on retry.
    """
    global current_retry_ride_id

    match_start = time.time()
    time.sleep(random.uniform(0.2, 0.6))

    px = ride["pickup_lon"]
    py = ride["pickup_lat"]
    ride_region = get_region(px, py)

    def txn(cur):
        cur.execute(
            """
            SELECT driver_id, current_lon, current_lat
            FROM drivers
            WHERE status = 'AVAILABLE'
              AND (region = %s OR region IS NULL)
            ORDER BY random()
            LIMIT 50;
            """,
            (ride_region,),
        )
        candidates = cur.fetchall()
        if not candidates:
            return None

        nearest = sorted(
            candidates,
            key=lambda r: _haversine(px, py, r[1], r[2])
        )[:MAX_NEAREST_DRIVERS]

        for driver in nearest:
            driver_id = driver[0]

            cur.execute(
                "SELECT status FROM drivers WHERE driver_id = %s FOR UPDATE;",
                (driver_id,),
            )
            row = cur.fetchone()
            if not row or row[0] != "AVAILABLE":
                continue

            match_latency_ms = round((time.time() - match_start) * 1000, 2)

            cur.execute(
                """
                UPDATE drivers
                SET status = 'MATCHING',
                    current_lon = %s,
                    current_lat = %s,
                    region      = %s,
                    last_updated = NOW()
                WHERE driver_id = %s AND status = 'AVAILABLE';
                """,
                (px, py, ride_region, driver_id),
            )

            print(f"[match_ride] Driver {driver_id} -> MATCHING")

            cur.execute(
                """
                UPDATE rides_p
                SET assigned_driver  = %s,
                    assigned_at      = NOW(),
                    match_latency_ms = %s,
                    retries          = 0,
                    region           = %s
                WHERE ride_id = %s
                  AND status = 'REQUESTED';
                """,
                (driver_id, match_latency_ms, ride_region, ride["ride_id"]),
            )

            return driver_id

        return None

    # ------------- IMPORTANT PATCH ----------------
    try:
        current_retry_ride_id = ride["ride_id"]      # <<< Tell run_txn which ride is current
        return run_txn(txn)
    finally:
        current_retry_ride_id = None                 # <<< Always clean up global variable
    # ------------------------------------------------


def complete_ride(ride, driver_id, simulated_duration_seconds):
    def txn(cur):
        distance_km = _haversine(
            ride["pickup_lon"],
            ride["pickup_lat"],
            ride["dropoff_lon"],
            ride["dropoff_lat"],
        )
        fare = calculate_fare(distance_km)

        start_time = ride["pickup_datetime"]
        end_time = start_time + timedelta(seconds=simulated_duration_seconds)

        drop_region = get_region(ride["dropoff_lon"], ride["dropoff_lat"])

        cur.execute(
            """
            INSERT INTO trips_p (
                ride_id, driver_id, start_time, end_time,
                total_amount, distance
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ride_id) DO NOTHING;
            """,
            (ride["ride_id"], driver_id, start_time, end_time, fare, distance_km),
        )

        cur.execute(
            "UPDATE rides_p SET status = 'COMPLETED', retries = 0 WHERE ride_id = %s;",
            (ride["ride_id"],),
        )

        cur.execute(
            """
            UPDATE drivers
            SET status       = 'AVAILABLE',
                current_lon  = %s,
                current_lat  = %s,
                region       = %s,
                last_updated = NOW()
            WHERE driver_id = %s;
            """,
            (ride["dropoff_lon"], ride["dropoff_lat"], drop_region, driver_id),
        )

    run_txn(txn)
