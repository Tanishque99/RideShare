# src/replayer.py
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from matcher import match_ride, complete_ride, busy_drivers  # Redis sync handled in matcher
from db import get_cursor

# Simulation constants
SIMULATION_SPEEDUP = 30
MIN_SIM_DURATION_SEC = 2
MAX_CONCURRENCY = 500  # Sync with Cockroach CPU cores / thread pool

# Retry window = 5 minutes (keep retry active until time expires)
MAX_WAIT_SECONDS = 5 * 60


def process_ride(row, idx):
    try:
        (
            ride_id,
            pickup_dt,
            dropoff_dt,
            pax,
            dist,
            plo,
            pla,
            dlo,
            dla,
            amt,
        ) = row

        ride = {
            "ride_id": ride_id,
            "pickup_datetime": pickup_dt,
            "dropoff_datetime": dropoff_dt,
            "pickup_lon": plo,
            "pickup_lat": pla,
            "dropoff_lon": dlo,
            "dropoff_lat": dla,
            "passenger_count": pax,
        }

        # Insert ride as REQUESTED
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO rides_p (
                    ride_id, requested_at,
                    pickup_lon, pickup_lat,
                    dropoff_lon, dropoff_lat,
                    passenger_count, status, retries
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'REQUESTED', 0)
                ON CONFLICT (ride_id) DO UPDATE
                SET status       = 'REQUESTED',
                    requested_at = EXCLUDED.requested_at;
                """,
                (ride_id, pickup_dt, plo, pla, dlo, dla, pax),
            )

        print(f"[Thread-{idx}] 📥 REQUESTED → {ride_id}")

        first_attempt_ts = time.time()
        driver_id = None

        # Try matching until timeout
        while True:
            driver_id = match_ride(ride)

            if driver_id:
                print(f"[Thread-{idx}] 🚖 MATCHED → Driver {driver_id}")

                # 🔥 FIX #1: Mark as ASSIGNED immediately
                with get_cursor(commit=True) as cur:
                    cur.execute(
                        "UPDATE rides_p SET status='ASSIGNED', retries=0 WHERE ride_id=%s;",
                        (ride_id,),
                    )
                break

            elapsed = time.time() - first_attempt_ts

            # Track retry counter
            with get_cursor(commit=True) as cur:
                cur.execute(
                    """
                    UPDATE rides_p
                    SET retries = retries + 1
                    WHERE ride_id = %s
                      AND assigned_driver IS NULL
                      AND status = 'REQUESTED';
                    """,
                    (ride_id,),
                )

            if elapsed >= MAX_WAIT_SECONDS:
                print(f"[Thread-{idx}] ❌ EXPIRED → {ride_id} ({elapsed:.1f}s no match)")
                with get_cursor(commit=True) as cur:
                    cur.execute(
                        """
                        UPDATE rides_p
                        SET status = 'EXPIRED'
                        WHERE ride_id = %s
                          AND assigned_driver IS NULL
                          AND status = 'REQUESTED';
                        """,
                        (ride_id,),
                    )
                return

            time.sleep(random_backoff())

        # Move ride to EN_ROUTE stage
        if driver_id:
            with get_cursor(commit=True) as cur:
                cur.execute(
                    "UPDATE rides_p SET status = 'EN_ROUTE', retries = 0 WHERE ride_id = %s;",
                    (ride_id,),
                )

            real_duration = (dropoff_dt - pickup_dt).total_seconds()
            simulated_duration = max(real_duration / SIMULATION_SPEEDUP, MIN_SIM_DURATION_SEC)

            print(f"[Thread-{idx}] 🚗 EN_ROUTE ({simulated_duration:.2f}s simulated)")
            time.sleep(simulated_duration)

            complete_ride(ride, driver_id, simulated_duration)

            # 🔥 FIX #2: Remove redundant busy_drivers cleanup (already released inside complete_ride)
            # ❌ Removed:
            # if driver_id in busy_drivers:
            #     busy_drivers.discard(driver_id)

    except Exception as e:
        print(f"[Thread-{idx}] ❌ ERROR during ride processing: {e}")


# ---- Utility ----
def random_backoff():
    """Optional jitter to reduce simultaneous retries."""
    import random
    return random.uniform(0.8, 1.3)


# ===== Main Replayer =====
def replayer(limit=50):
    print(f"[replayer] 🎬 Starting simulation of {limit} rides...")

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT ride_id, pickup_datetime, dropoff_datetime, passenger_count,
                   trip_distance, pickup_lon, pickup_lat,
                   dropoff_lon, dropoff_lat, total_amount
            FROM nyc_clean
            ORDER BY pickup_datetime
            LIMIT %s;
            """,
            (limit,),
        )
        rides = cur.fetchall()

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
        futures = {
            executor.submit(process_ride, ride, i): i
            for i, ride in enumerate(rides, start=1)
        }

        for future in as_completed(futures):
            idx = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"[Thread-{idx}] ⚠ Unexpected exception: {e}")

    print("[replayer] 🎉 All rides processed.")


# ===== Standalone Run =====
if __name__ == "__main__":
    replayer(50)
