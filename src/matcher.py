# src/matcher.py
import math
import time
import random
from datetime import timedelta
from db import run_txn

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

def match_ride(ride):
    match_start = time.time()

    #  NEW → simulate real-world matching delay so ride stays in REQUESTED
    time.sleep(random.uniform(0.5, 1.5))  # 500–1500ms

    #  NEW → keep 10% of rides as pending (REQUESTED) to show queue
    if random.random() < 0.10:
        return None

    def txn(cur):
        cur.execute("""
            SELECT driver_id, current_lon, current_lat
            FROM drivers
            WHERE status='AVAILABLE'
            ORDER BY random()
            LIMIT 50;
        """)
        candidates = cur.fetchall()
        if not candidates:
            return None

        px, py = ride["pickup_lon"], ride["pickup_lat"]
        nearest_drivers = sorted(
            candidates,
            key=lambda r: _haversine(px, py, r[1], r[2])
        )[:MAX_NEAREST_DRIVERS]

        for driver in nearest_drivers:
            driver_id = driver[0]

            cur.execute("SELECT status FROM drivers WHERE driver_id=%s FOR UPDATE;", (driver_id,))
            if cur.fetchone()[0] != "AVAILABLE":
                continue

            match_latency_ms = round((time.time() - match_start) * 1000, 2)

            #  We DO NOT change status to ASSIGNED directly!
            # Just assign driver & keep EN_ROUTE change in replayer.py
            cur.execute("""
                UPDATE rides_p
                SET assigned_driver=%s,
                    assigned_at=NOW(),
                    match_latency_ms=%s
                WHERE ride_id=%s AND status='REQUESTED';
            """, (driver_id, match_latency_ms, ride["ride_id"]))

            cur.execute("""
                UPDATE drivers
                SET status='EN_ROUTE',
                    current_lon=%s,
                    current_lat=%s,
                    last_updated=NOW()
                WHERE driver_id=%s;
            """, (px, py, driver_id))

            return driver_id

        return None

    return run_txn(txn)


def complete_ride(ride, driver_id, simulated_duration_seconds):
    def txn(cur):
        distance = _haversine(
            ride["pickup_lon"], ride["pickup_lat"],
            ride["dropoff_lon"], ride["dropoff_lat"]
        )
        fare = calculate_fare(distance)

        start_time = ride["pickup_datetime"]
        end_time = start_time + timedelta(seconds=simulated_duration_seconds)

        cur.execute("""
            INSERT INTO trips_p (ride_id, driver_id, start_time, end_time, total_amount, distance)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ride_id) DO NOTHING;
        """, (ride["ride_id"], driver_id, start_time, end_time, fare, distance))

        cur.execute("UPDATE rides_p SET status='COMPLETED' WHERE ride_id=%s;", (ride["ride_id"],))

        cur.execute("""
            UPDATE drivers
            SET status='AVAILABLE',
                current_lon=%s,
                current_lat=%s,
                last_updated=NOW()
            WHERE driver_id=%s;
        """, (ride["dropoff_lon"], ride["dropoff_lat"], driver_id))

    run_txn(txn)
