# src/replayer.py
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import json
from datetime import datetime

from matcher import match_ride, complete_ride
from db import get_cursor

SIMULATION_SPEEDUP = 30
MIN_SIM_DURATION_SEC = 2
MAX_CONCURRENCY = 300
MAX_WAIT_SECONDS = 5 * 60  # Retry for up to 5 minutes


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

        # 📌 Insert ride as REQUESTED
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
            """, (ride_id, pickup_dt, plo, pla, dlo, dla, pax))

        print(f"[Thread-{idx}] 📥 REQUESTED -> {ride_id}")

        first_attempt_ts = time.time()
        driver_id = None

        # 🔁 Attempt matching until timeout
        while True:
            driver_id = match_ride(ride)

            if driver_id:
                print(f"[Thread-{idx}] 🚖 Matched -> Driver {driver_id}")
                break

            elapsed = time.time() - first_attempt_ts
            if elapsed >= MAX_WAIT_SECONDS:
                print(f"[Thread-{idx}] ⛔ Ride {ride_id} expired after {elapsed:.1f}s")
                with get_cursor(commit=True) as cur:
                    cur.execute("""
                        UPDATE rides_p
                        SET status = 'EXPIRED'
                        WHERE ride_id = %s
                        AND assigned_driver IS NULL
                        AND status = 'REQUESTED';
                    """, (ride_id,))
                return

            # Set retry indicator
            with get_cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE rides_p
                    SET retries = 1
                    WHERE ride_id = %s
                      AND assigned_driver IS NULL
                      AND status = 'REQUESTED';
                """, (ride_id,))

            print(f"[Thread-{idx}] 🔁 Retrying ride {ride_id} ...")
            time.sleep(1)

        # Transition MATCHING -> EN_ROUTE and update ride status
        if driver_id:
            with get_cursor(commit=True) as cur:
                cur.execute(
                    """
                    UPDATE drivers
                    SET status='EN_ROUTE'
                    WHERE driver_id=%s AND status='MATCHING';
                    """,
                    (driver_id,),
                )

                cur.execute(
                    """
                    UPDATE rides_p
                    SET status='EN_ROUTE', retries = 0
                    WHERE ride_id=%s;
                """, (ride_id,))

            real_duration = (dropoff_dt - pickup_dt).total_seconds()
            simulated_duration = max(real_duration / SIMULATION_SPEEDUP, MIN_SIM_DURATION_SEC)

            print(f"[Thread-{idx}] 🚗 EN_ROUTE (sim={simulated_duration:.2f}s)")
            time.sleep(simulated_duration)

            complete_ride(ride, driver_id, simulated_duration)

    except Exception as e:
        print(f"[Thread-{idx}] ❌ ERROR in process_ride: {e}")


# New helper: collect replica distribution and query performance metrics from Postgres
def collect_db_metrics():
    """
    Collect:
      - replica_distribution: connection counts per replication client (pg_stat_replication)
      - top_queries: top statements from pg_stat_statements (if available)
    Returns a dict suitable for JSON output.
    """
    metrics = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "replica_distribution": {},
        "top_queries": [],
    }

    try:
        with get_cursor() as cur:
            # Replica distribution (may be empty/unavailable)
            try:
                cur.execute("""
                    SELECT COALESCE(client_addr::text, 'local') AS node, count(*) AS connections
                    FROM pg_stat_replication
                    GROUP BY client_addr;
                """)
                rows = cur.fetchall()
                for node, cnt in rows:
                    metrics["replica_distribution"][str(node)] = int(cnt)
            except Exception:
                metrics["replica_distribution_error"] = "pg_stat_replication unavailable"

            # Top queries (requires pg_stat_statements extension)
            try:
                cur.execute("""
                    SELECT query, calls, total_time, mean_time
                    FROM pg_stat_statements
                    ORDER BY total_time DESC
                    LIMIT 10;
                """)
                qrows = cur.fetchall()
                for q, calls, total_time, mean_time in qrows:
                    metrics["top_queries"].append({
                        "query_snippet": (q or "")[:300],
                        "calls": int(calls or 0),
                        "total_time_ms": float(total_time or 0.0),
                        "mean_time_ms": float(mean_time or 0.0),
                    })
            except Exception:
                metrics["top_queries_error"] = "pg_stat_statements unavailable"
    except Exception as e:
        metrics["error"] = str(e)

    return metrics


def replayer(limit=50):
    print(f"[replayer] Starting for {limit or 'ALL'} rides...")

    # build query without LIMIT when limit is None/Falsey
    with get_cursor() as cur:
        cur.execute("""
            SELECT ride_id, pickup_datetime, dropoff_datetime, passenger_count,
                   trip_distance, pickup_lon, pickup_lat,
                   dropoff_lon, dropoff_lat, total_amount
            FROM nyc_clean
            ORDER BY pickup_datetime
            LIMIT %s;
        """, (limit,))
        rides = cur.fetchall()

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
        futures = {executor.submit(process_ride, ride, i): i for i, ride in enumerate(rides, start=1)}

        for future in as_completed(futures):
            idx = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"[Thread-{idx}] ❌ Unexpected error: {e}")

    print("[replayer] ✅ All rides processed.")


if __name__ == "__main__":
    replayer(50)
