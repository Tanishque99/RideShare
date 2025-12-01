# src/matcher.py

import math
import time
import random
from datetime import timedelta

from db import run_txn, get_cursor

# 🔥 Redis for in-memory coordination
import redis
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
BUSY_KEY = "busy_drivers"  # Redis set name

EARTH_R = 6371.0
MAX_NEAREST_DRIVERS = 5

# Local cache (just for speed)
busy_drivers = set()


# ------------------ HELPER FUNCTIONS ------------------

def _haversine(lon1, lat1, lon2, lat2):
    """Calculate distance between two geo-coordinates."""
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
    """Simple fare model: base + per km."""
    return round(3.0 + distance_km * 1.8, 2)


# ------------------ MATCHING LOGIC ------------------

def mark_driver_busy(driver_id):
    """Store driver's 'busy' status in Redis + local memory."""
    r.sadd(BUSY_KEY, driver_id)
    busy_drivers.add(driver_id)


def is_driver_busy(driver_id):
    """Check Redis (authoritative) and sync to local cache."""
    if r.sismember(BUSY_KEY, driver_id):
        busy_drivers.add(driver_id)
        return True
    return False


def unmark_driver_busy(driver_id):
    """Mark driver available again."""
    r.srem(BUSY_KEY, driver_id)
    busy_drivers.discard(driver_id)


def match_ride(ride: dict):
    """
    Try to match a ride to an AVAILABLE driver.
    Returns driver_id or None.
    """
    match_start = time.time()
    time.sleep(random.uniform(0.2, 0.6))  # artificial matching delay

    def txn(cur):
        # 1) Get random available candidates
        cur.execute("""
            SELECT driver_id, current_lon, current_lat
            FROM drivers
            WHERE status = 'AVAILABLE'
            ORDER BY random()
            LIMIT 50;
        """)
        candidates = cur.fetchall()
        if not candidates:
            return None

        px, py = ride["pickup_lon"], ride["pickup_lat"]

        # 2) Sort by proximity
        nearest = sorted(
            candidates,
            key=lambda r: _haversine(px, py, r[1], r[2])
        )[:MAX_NEAREST_DRIVERS]

        for driver in nearest:
            driver_id = driver[0]

            # Skip if busy in Redis
            if is_driver_busy(driver_id):
                continue

            # Lock before checking availability
            cur.execute("SELECT status FROM drivers WHERE driver_id=%s FOR UPDATE;", (driver_id,))
            row = cur.fetchone()
            if not row or row[0] != "AVAILABLE":
                continue

            match_latency_ms = round((time.time() - match_start) * 1000, 2)

            # Move to MATCHING stage (do NOT go to EN_ROUTE here)
            cur.execute("""
                UPDATE drivers
                SET status='MATCHING',
                    current_lon=%s,
                    current_lat=%s,
                    last_updated=NOW()
                WHERE driver_id=%s;
            """, (px, py, driver_id))

            # Assign ride
            cur.execute("""
                UPDATE rides_p
                SET assigned_driver=%s,
                    assigned_at=NOW(),
                    status='ASSIGNED',       
                    match_latency_ms=%s,
                    retries=0
                WHERE ride_id=%s AND status='REQUESTED';
            """, (driver_id, match_latency_ms, ride["ride_id"]))

            # 🔥 Mark as busy in Redis
            mark_driver_busy(driver_id)

            return driver_id

        return None  # no drivers matched

    try:
        return run_txn(txn)
    except Exception as e:
        print(f"[match_ride] Cockroach retry conflict on ride {ride['ride_id']}: {e}")
        return None


# ------------------ COMPLETION LOGIC ------------------

def complete_ride(ride, driver_id, simulated_duration_seconds):
    """Mark the ride completed + driver AVAILABLE again."""
    def txn(cur):
        distance_km = _haversine(
            ride["pickup_lon"], ride["pickup_lat"],
            ride["dropoff_lon"], ride["dropoff_lat"]
        )
        fare = calculate_fare(distance_km)

        start_time = ride["pickup_datetime"]
        end_time = start_time + timedelta(seconds=simulated_duration_seconds)

        # Insert trip
        cur.execute("""
            INSERT INTO trips_p (
                ride_id, driver_id, start_time, end_time,
                total_amount, distance
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ride_id) DO NOTHING;
        """, (ride["ride_id"], driver_id, start_time, end_time, fare, distance_km))

        # Mark ride as completed
        cur.execute("""
            UPDATE rides_p SET status='COMPLETED', retries=0 WHERE ride_id=%s;
        """, (ride["ride_id"],))

        # Free driver
        cur.execute("""
            UPDATE drivers
            SET status='AVAILABLE',
                current_lon=%s,
                current_lat=%s,
                last_updated=NOW()
            WHERE driver_id=%s;
        """, (ride["dropoff_lon"], ride["dropoff_lat"], driver_id))

    run_txn(txn)

    # 🔥 Final release from Redis lock
    unmark_driver_busy(driver_id)


# -------------------------------------------------------
if __name__ == "__main__":
    print("matcher.py loaded. No standalone execution.")
