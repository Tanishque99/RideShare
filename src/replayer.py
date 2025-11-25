# src/replayer.py
import time
from db import get_cursor
from matcher import match_ride, complete_ride

ASSIGN_TO_COMPLETE_DELAY = 5  # seconds

def replayer(limit=10):
    print(f"[replayer] Fetching up to {limit} rides from nyc_clean...")
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
        rows = cur.fetchall()

    print(f"[replayer] Retrieved {len(rows)} rides. Starting replay...")

    for idx, row in enumerate(rows, start=1):
        (ride_id, pickup_dt, dropoff_dt, pax, dist,
         plo, pla, dlo, dla, amt) = row

        ride = {
            "ride_id": ride_id,
            "pickup_datetime": pickup_dt,
            "pickup_lon": plo,
            "pickup_lat": pla,
            "dropoff_lon": dlo,
            "dropoff_lat": dla,
            "passenger_count": pax,
        }

        driver_id = match_ride(ride)

        if not driver_id:
            print(f"[replayer] #{idx} {pickup_dt} :: NO DRIVER")
            continue

        print(f"[replayer] #{idx} {pickup_dt} :: ASSIGNED {ride_id} -> {driver_id}")
        time.sleep(ASSIGN_TO_COMPLETE_DELAY)

        complete_ride(ride, driver_id)
        print(f"[replayer] #{idx} {pickup_dt} :: COMPLETED {ride_id} for {driver_id}")

        time.sleep(1)


if __name__ == "__main__":
    replayer(10)
