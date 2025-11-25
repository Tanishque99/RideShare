# src/matcher.py
import math
from db import run_txn

EARTH_R = 6371.0  # km

def _haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    return EARTH_R * c


def match_ride(ride):
    """
    Assign nearest AVAILABLE driver to the ride in a serializable transaction.
    """
    def txn(cur):
        # 1) candidate drivers
        cur.execute("""
            SELECT driver_id, current_lon, current_lat
            FROM drivers
            WHERE status = 'AVAILABLE'
            ORDER BY random()
            LIMIT 100;
        """)
        candidates = cur.fetchall()
        if not candidates:
            return None

        px, py = ride["pickup_lon"], ride["pickup_lat"]
        best = min(candidates, key=lambda row: _haversine(px, py, row[1], row[2]))
        driver_id = best[0]

        # 2) lock driver
        cur.execute("""
            SELECT status FROM drivers
            WHERE driver_id = %s
            FOR UPDATE;
        """, (driver_id,))
        row = cur.fetchone()
        if row is None or row[0] != "AVAILABLE":
            raise Exception("Driver already taken")

        # 3) insert ride as ASSIGNED
        cur.execute("""
            INSERT INTO rides_p (
              ride_id, requested_at,
              pickup_lon, pickup_lat,
              dropoff_lon, dropoff_lat,
              passenger_count, assigned_driver, status
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'ASSIGNED');
        """, (
            ride["ride_id"],
            ride["pickup_datetime"],
            ride["pickup_lon"], ride["pickup_lat"],
            ride["dropoff_lon"], ride["dropoff_lat"],
            ride["passenger_count"],
            driver_id
        ))

        # 4) set driver EN_ROUTE
        cur.execute("""
            UPDATE drivers
            SET status = 'EN_ROUTE',
                current_lon = %s,
                current_lat = %s,
                last_updated = now()
            WHERE driver_id = %s;
        """, (ride["pickup_lon"], ride["pickup_lat"], driver_id))

        return driver_id

    return run_txn(txn)


def complete_ride(ride, driver_id):
    """
    Minimal: just mark ride COMPLETED and driver AVAILABLE.
    """
    def txn(cur):
        # 1) update ride status
        cur.execute("""
            UPDATE rides_p
            SET status = 'COMPLETED'
            WHERE ride_id = %s;
        """, (ride["ride_id"],))

        # 2) free driver
        cur.execute("""
            UPDATE drivers
            SET status = 'AVAILABLE',
                current_lon = %s,
                current_lat = %s,
                last_updated = now()
            WHERE driver_id = %s;
        """, (ride["dropoff_lon"], ride["dropoff_lat"], driver_id))

    run_txn(txn)
