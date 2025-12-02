# src/replayer.py
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from matcher import match_ride, complete_ride
from db import get_cursor
from geo import get_region   # NEW


# src/replayer.py

SIMULATION_SPEEDUP = 30          # was 30 → trips last longer
MIN_SIM_DURATION_SEC = 3        # was 2  → drivers stay busy longer
MAX_CONCURRENCY = 200            # was 10000 → avoid 10k threads on your laptop
TIMEOUT_SECONDS = 300            # you can keep this or drop if unused


def process_ride(row, idx):
    try:
        (ride_id, pickup_dt, dropoff_dt, pax, dist,
         plo, pla, dlo, dla, amt) = row

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

        ride_region = get_region(plo, pla)  # NEW


        # 📌 Insert as REQUESTED ONLY
        # 📌 Insert ride as REQUESTED
        with get_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO rides_p (
                    ride_id, requested_at,
                    pickup_lon, pickup_lat,
                    dropoff_lon, dropoff_lat,
                    passenger_count, region, status, retries
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'REQUESTED', 0)
                ON CONFLICT (ride_id) DO UPDATE
                SET status       = 'REQUESTED',
                    requested_at = EXCLUDED.requested_at,
                    region       = EXCLUDED.region;
            """, (ride_id, pickup_dt, plo, pla, dlo, dla, pax, ride_region))


        print(f"[Thread-{idx}] 📥 REQUESTED -> {ride_id}")

        sim_start = time.time()

        while True:
            driver_id = match_ride(ride)
            if driver_id:
                print(f"[Thread-{idx}] 🚖 Matched -> Driver {driver_id}")
                break
            time.sleep(1)

        # Change to EN_ROUTE after assignment
        with get_cursor(commit=True) as cur:
            cur.execute("UPDATE rides_p SET status='EN_ROUTE' WHERE ride_id=%s;", (ride_id,))

        real_duration = (dropoff_dt - pickup_dt).total_seconds()
        simulated_duration = max(real_duration / SIMULATION_SPEEDUP, MIN_SIM_DURATION_SEC)
        print(f"[Thread-{idx}]  EN_ROUTE ({simulated_duration:.2f}s)")
        time.sleep(simulated_duration)

        complete_ride(ride, driver_id, simulated_duration)

    except Exception as e:
        print(f"[Thread-{idx}]  ERROR: {e}")


def replayer(limit=50):
    print(f"[replayer] Starting for {limit} rides...")

    with get_cursor() as cur:
        cur.execute("""
            SELECT ride_id, pickup_datetime, dropoff_datetime, passenger_count,
                   trip_distance, pickup_lon, pickup_lat, dropoff_lon, dropoff_lat, total_amount
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
                print(f"[Thread-{idx}]  Unexpected error: {e}")

    print("[replayer]  All rides processed.")


if __name__ == "__main__":
    replayer(50)
