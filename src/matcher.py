# src/matcher.py
import math
import time
import random
from datetime import timedelta, datetime

from db import run_txn, get_cursor

EARTH_R = 6371.0
MAX_NEAREST_DRIVERS = 5


def _haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    return EARTH_R * (
        2
        * math.asin(
            math.sqrt(
                math.sin((lat2 - lat1) / 2) ** 2
                + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
            )
        )
    )


def calculate_fare(distance_km: float) -> float:
    # Simple fare model: base + per-km
    return round(3.0 + distance_km * 1.8, 2)


def match_ride(ride: dict):
    """
    Try to match a ride to an AVAILABLE driver.

    Returns:
        driver_id (str/int) on success, or None if no driver could be matched
        in this attempt.

    NOTE: We DO NOT do time-based stopping here – that’s handled in replayer.py.
    """
    match_start = time.time()

    # Just to make latency graph more interesting (but not huge):
    time.sleep(random.uniform(0.2, 0.6))

    def txn(cur):
        # 1) Fetch candidate drivers
        cur.execute(
            """
            SELECT driver_id, current_lon, current_lat
            FROM drivers
            WHERE status = 'AVAILABLE'
            ORDER BY random()
            LIMIT 50;
            """
        )
        candidates = cur.fetchall()
        if not candidates:
            return None

        px, py = ride["pickup_lon"], ride["pickup_lat"]

        # 2) Sort by distance and keep TOP N
        nearest = sorted(
            candidates,
            key=lambda r: _haversine(px, py, r[1], r[2])
        )[:MAX_NEAREST_DRIVERS]

        # 3) Try to lock & assign one driver
        for driver in nearest:
            driver_id = driver[0]

            # Lock row to avoid double-assignment
            cur.execute(
                "SELECT status FROM drivers WHERE driver_id = %s FOR UPDATE;",
                (driver_id,),
            )
            row = cur.fetchone()
            if not row or row[0] != "AVAILABLE":
                continue

            match_latency_ms = round((time.time() - match_start) * 1000, 2)

            # 🚀 NEW: Mark driver as MATCHING before EN_ROUTE
            cur.execute(
                """
                UPDATE drivers
                SET status = 'MATCHING',
                    current_lon = %s,
                    current_lat = %s,
                    last_updated = NOW()
                WHERE driver_id = %s AND status = 'AVAILABLE';
                """,
                (px, py, driver_id),
            )

            print(f"[match_ride] Driver {driver_id} -> MATCHING")

            # Assign driver to ride
            cur.execute(
                """
                UPDATE rides_p
                SET assigned_driver = %s,
                    assigned_at     = NOW(),
                    match_latency_ms = %s,
                    retries         = 0
                WHERE ride_id = %s
                  AND status = 'REQUESTED';
                """,
                (driver_id, match_latency_ms, ride["ride_id"]),
            )

            return driver_id  # driver is now MATCHING, waiting for replayer to switch to EN_ROUTE.

        return None  # No driver assigned

    # Run Cockroach transaction and handle conflicts
    try:
        return run_txn(txn)
    except Exception as e:
        # Transaction conflict / error – count as a DB-level retry signal
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                UPDATE rides_p
                SET retries = COALESCE(retries, 0) + 1
                WHERE ride_id = %s;
                """,
                (ride["ride_id"],),
            )
        print(f"[match_ride] Cockroach retry for ride {ride['ride_id']}: {e}")
        return None


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

        # Insert trip row once per ride
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

        # Mark ride as completed
        cur.execute(
            "UPDATE rides_p SET status = 'COMPLETED', retries = 0 WHERE ride_id = %s;",
            (ride["ride_id"],),
        )

        # Free driver
        cur.execute(
            """
            UPDATE drivers
            SET status       = 'AVAILABLE',
                current_lon  = %s,
                current_lat  = %s,
                last_updated = NOW()
            WHERE driver_id = %s;
            """,
            (ride["dropoff_lon"], ride["dropoff_lat"], driver_id),
        )

    run_txn(txn)
