# src/matcher.py

import math
import time
import random
import logging
from datetime import timedelta

from db import run_txn, get_cursor

# 🔥 Redis for in-memory coordination
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
BUSY_KEY = "busy_drivers"  # Redis set name

EARTH_R = 6371.0
MAX_NEAREST_DRIVERS = 10  # Increased to have more fallback options

# Logging for debugging (can be set to WARNING for production)
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(message)s"
)
logger = logging.getLogger("matcher")
logger.setLevel(logging.WARNING)  # Reduced logging for large-scale test


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


def try_acquire_driver(driver_id):
    """
    🔥 FIX #1: Atomically try to mark driver as busy in Redis.
    Returns True if successfully acquired (driver was not busy), False otherwise.
    This prevents race conditions by using Redis SADD's atomic behavior.
    """
    driver_id_str = str(driver_id)
    # SADD returns 1 if the element was added (not already present), 0 if already present
    added = r.sadd(BUSY_KEY, driver_id_str)
    if added:
        logger.debug(
            f"[DIAG] try_acquire_driver({driver_id}) = SUCCESS (atomically added to Redis)"
        )
        return True
    else:
        logger.debug(
            f"[DIAG] try_acquire_driver({driver_id}) = FAILED (already in Redis)"
        )
        return False


def mark_driver_busy(driver_id):
    """Store driver's 'busy' status in Redis (for explicit marking after DB update)."""
    driver_id_str = str(driver_id)
    logger.debug(f"[DIAG] mark_driver_busy({driver_id}) - Ensuring in Redis")
    r.sadd(BUSY_KEY, driver_id_str)
    redis_size = r.scard(BUSY_KEY)
    logger.debug(f"[DIAG] Redis busy_drivers size: {redis_size}")


def is_driver_busy(driver_id):
    """Check Redis (authoritative source of truth)."""
    driver_id_str = str(driver_id)
    redis_busy = r.sismember(BUSY_KEY, driver_id_str)
    logger.debug(f"[DIAG] is_driver_busy({driver_id}) = {redis_busy}")
    return redis_busy


def release_driver(driver_id):
    """
    🔥 FIX #2: Release driver back to available pool.
    Called when matching fails or ride completes.
    """
    driver_id_str = str(driver_id)
    logger.debug(f"[DIAG] release_driver({driver_id}) - Removing from Redis")
    removed = r.srem(BUSY_KEY, driver_id_str)
    if not removed:
        logger.warning(f"[DIAG] Driver {driver_id} was NOT in Redis when releasing!")
    return removed


def unmark_driver_busy(driver_id):
    """Alias for release_driver for backward compatibility."""
    return release_driver(driver_id)


def get_busy_driver_ids():
    """Get all currently busy driver IDs from Redis."""
    return r.smembers(BUSY_KEY)


def initialize_redis_state():
    """
    🔥 FIX #6: Initialize Redis state to match current DB state.
    Should be called on application startup.
    """
    logger.info("[DIAG] Initializing Redis state...")
    try:
        # Clear Redis busy set
        r.delete(BUSY_KEY)

        # Get all drivers that are NOT available (busy, matching, en_route)
        with get_cursor() as cur:
            cur.execute(
                "SELECT driver_id::text FROM drivers WHERE status != 'AVAILABLE';"
            )
            busy_drivers = [row[0] for row in cur.fetchall()]

        if busy_drivers:
            # Add all busy drivers to Redis
            r.sadd(BUSY_KEY, *busy_drivers)
            logger.info(
                f"[DIAG] Initialized Redis with {len(busy_drivers)} busy drivers"
            )
        else:
            logger.info("[DIAG] No busy drivers found, Redis initialized empty")

    except Exception as e:
        logger.error(f"[DIAG] Failed to initialize Redis state: {e}")


def match_ride(ride: dict):
    """
    🔥 OPTIMIZED: Try to match a ride to an AVAILABLE driver.

    Key improvements:
    1. Acquire driver in Redis FIRST (atomic) before any DB operations
    2. Move artificial delay OUTSIDE the transaction
    3. Use READ COMMITTED for initial query, only lock the selected driver
    4. Release Redis lock if DB transaction fails

    Returns driver_id or None.
    """
    match_start = time.time()
    ride_id = ride["ride_id"]

    logger.info(f"[DIAG] ========== match_ride START for ride {ride_id} ==========")

    # 🔍 DIAGNOSTIC: Log Redis state at start
    redis_busy_count = r.scard(BUSY_KEY)
    logger.info(f"[DIAG] Redis busy_drivers count: {redis_busy_count}")

    # 🔥 FIX #3: Move artificial delay OUTSIDE the transaction to reduce conflict window
    time.sleep(random.uniform(0.1, 0.3))  # Reduced delay, outside txn

    px, py = ride["pickup_lon"], ride["pickup_lat"]

    # 🔥 FIX #4: Get candidates OUTSIDE the main transaction (read-only, no lock needed)
    candidates = []
    try:
        with get_cursor() as cur:
            # Get busy driver IDs from Redis to exclude them from the query
            busy_ids = get_busy_driver_ids()

            if busy_ids:
                # 🔥 FIX #1: Filter out busy drivers at the SQL level
                placeholders = ",".join(["%s"] * len(busy_ids))
                cur.execute(
                    f"""
                    SELECT driver_id, current_lon, current_lat
                    FROM drivers
                    WHERE status = 'AVAILABLE'
                    AND driver_id::text NOT IN ({placeholders})
                    ORDER BY random()
                    LIMIT 50;
                """,
                    tuple(busy_ids),
                )
            else:
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
    except Exception as e:
        logger.error(f"[DIAG] Ride {ride_id}: Failed to fetch candidates: {e}")
        return None

    query_time = (time.time() - match_start) * 1000
    logger.info(
        f"[DIAG] Ride {ride_id}: Found {len(candidates)} AVAILABLE drivers (query took {query_time:.2f}ms)"
    )

    if not candidates:
        logger.warning(f"[DIAG] Ride {ride_id}: NO available drivers!")
        return None

    # Sort by proximity
    nearest = sorted(candidates, key=lambda d: _haversine(px, py, d[1], d[2]))[
        :MAX_NEAREST_DRIVERS
    ]

    logger.info(f"[DIAG] Ride {ride_id}: Checking {len(nearest)} nearest drivers")

    drivers_skipped_redis = 0
    drivers_skipped_db = 0
    acquired_driver_id = None

    # 🔥 FIX #2: Try to acquire driver in Redis FIRST (atomic operation)
    for driver in nearest:
        driver_id = driver[0]

        # Atomically try to acquire this driver in Redis
        if try_acquire_driver(driver_id):
            acquired_driver_id = driver_id
            logger.debug(f"[DIAG] Ride {ride_id}: Acquired driver {driver_id} in Redis")
            break
        else:
            drivers_skipped_redis += 1
            logger.debug(
                f"[DIAG] Ride {ride_id}: Driver {driver_id} already busy in Redis"
            )

    if not acquired_driver_id:
        logger.warning(
            f"[DIAG] Ride {ride_id}: Could not acquire any driver in Redis (skipped {drivers_skipped_redis})"
        )
        return None

    # Now we have exclusive Redis lock on this driver, proceed with DB transaction
    def txn(cur):
        nonlocal drivers_skipped_db

        # Verify driver is still AVAILABLE in DB and lock it
        cur.execute(
            "SELECT status FROM drivers WHERE driver_id=%s FOR UPDATE;",
            (acquired_driver_id,),
        )
        row = cur.fetchone()

        if not row or row[0] != "AVAILABLE":
            drivers_skipped_db += 1
            logger.warning(
                f"[DIAG] Ride {ride_id}: Driver {acquired_driver_id} not AVAILABLE in DB (status={row[0] if row else 'None'})"
            )
            return None  # Will release Redis lock in the except/finally block

        match_latency_ms = round((time.time() - match_start) * 1000, 2)

        # Update driver status to MATCHING
        cur.execute(
            """
            UPDATE drivers
            SET status='MATCHING',
                current_lon=%s,
                current_lat=%s,
                last_updated=NOW()
            WHERE driver_id=%s;
        """,
            (px, py, acquired_driver_id),
        )

        # Assign ride
        cur.execute(
            """
            UPDATE rides_p
            SET assigned_driver=%s,
                assigned_at=NOW(),
                status='ASSIGNED',
                match_latency_ms=%s,
                retries=0
            WHERE ride_id=%s AND status='REQUESTED';
        """,
            (acquired_driver_id, match_latency_ms, ride["ride_id"]),
        )

        logger.info(
            f"[DIAG] Ride {ride_id}: MATCHED to driver {acquired_driver_id} in {match_latency_ms}ms"
        )
        return acquired_driver_id

    try:
        result = run_txn(txn)

        if result is None:
            # DB said driver not available, release Redis lock
            logger.debug(
                f"[DIAG] Ride {ride_id}: Releasing Redis lock for driver {acquired_driver_id} (DB mismatch)"
            )
            release_driver(acquired_driver_id)
            return None

        total_time = (time.time() - match_start) * 1000
        logger.info(
            f"[DIAG] ========== match_ride END for ride {ride_id}: result={result}, total_time={total_time:.2f}ms =========="
        )
        return result

    except Exception as e:
        # Transaction failed, release Redis lock
        logger.error(
            f"[DIAG] Ride {ride_id}: TRANSACTION FAILED, releasing Redis lock for driver {acquired_driver_id}"
        )
        release_driver(acquired_driver_id)

        error_code = getattr(e, "pgcode", "unknown")
        logger.error(f"[DIAG] Ride {ride_id}: Error code={error_code}, error={e}")
        print(f"[match_ride] Cockroach retry conflict on ride {ride['ride_id']}: {e}")
        return None


# ------------------ COMPLETION LOGIC ------------------


def complete_ride(ride, driver_id, simulated_duration_seconds):
    """Mark the ride completed + driver AVAILABLE again."""
    ride_id = ride["ride_id"]
    logger.info(f"[DIAG] complete_ride START for ride {ride_id}, driver {driver_id}")

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

        # Insert trip
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
            """
            UPDATE rides_p SET status='COMPLETED', retries=0 WHERE ride_id=%s;
        """,
            (ride["ride_id"],),
        )

        # Free driver
        cur.execute(
            """
            UPDATE drivers
            SET status='AVAILABLE',
                current_lon=%s,
                current_lat=%s,
                last_updated=NOW()
            WHERE driver_id=%s;
        """,
            (ride["dropoff_lon"], ride["dropoff_lat"], driver_id),
        )

        logger.debug(
            f"[DIAG] Ride {ride_id}: DB updated - driver {driver_id} set to AVAILABLE"
        )

    run_txn(txn)

    # 🔥 Final release from Redis lock
    logger.debug(
        f"[DIAG] Ride {ride_id}: About to unmark driver {driver_id} from Redis"
    )
    unmark_driver_busy(driver_id)
    logger.info(
        f"[DIAG] complete_ride END for ride {ride_id}, driver {driver_id} released"
    )


# -------------------------------------------------------
if __name__ == "__main__":
    print("matcher.py loaded. No standalone execution.")
