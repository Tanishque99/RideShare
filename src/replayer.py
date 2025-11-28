# src/replayer.py
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from matcher import match_ride, complete_ride
from db import get_cursor

ASSIGN_TO_COMPLETE_DELAY = 3
MAX_CONCURRENCY = 10  # Number of rides processed in parallel

def process_ride(row, idx):
    try:
        (ride_id, pickup_dt, dropoff_dt, pax, dist, plo, pla, dlo, dla, amt) = row

        ride = {
            "ride_id": ride_id,
            "pickup_datetime": pickup_dt,
            "dropoff_datetime": dropoff_dt,
            "pickup_lon": plo,
            "pickup_lat": pla,
            "dropoff_lon": dlo,
            "dropoff_lat": dla,
            "passenger_count": pax,
            "distance": float(dist) if dist else None,
            "amount": float(amt) if amt else None
        }

        driver_id = match_ride(ride)
        if not driver_id:
            print(f"[Thread-{idx}] NO DRIVER AVAILABLE for {ride_id}")
            return

        print(f"[Thread-{idx}] ASSIGNED {ride_id} -> Driver {driver_id}")

        time.sleep(ASSIGN_TO_COMPLETE_DELAY)
        complete_ride(ride, driver_id)

        print(f"[Thread-{idx}] COMPLETED {ride_id}")
    except Exception as e:
        print(f"[Thread-{idx}] ERROR: {e}")


def replayer(limit=50):
    print(f"[replayer] Starting concurrent processing for {limit} rides...")

    with get_cursor() as cur:
        cur.execute("""
            SELECT
              ride_id,
              pickup_datetime,
              dropoff_datetime,
              passenger_count,
              trip_distance,
              pickup_lon,
              pickup_lat,
              dropoff_lon,
              dropoff_lat,
              total_amount
            FROM nyc_clean
            ORDER BY pickup_datetime
            LIMIT %s;
        """, (limit,))
        rides = cur.fetchall()

    print(f"[replayer] Retrieved {len(rides)} rides")

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
        futures = {executor.submit(process_ride, ride, i): i for i, ride in enumerate(rides, start=1)}

        for future in as_completed(futures):
            idx = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"[Thread-{idx}] Unexpected error: {e}")

    print("[replayer] All rides processed in concurrent mode.")


if __name__ == "__main__":
    replayer(50)
