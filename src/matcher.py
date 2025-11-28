# src/matcher.py
import math
from db import run_txn

EARTH_R = 6371.0

def _haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    return EARTH_R * (2 * math.asin(math.sqrt(
        math.sin(dlat / 2) ** 2 +
        math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )))

def calculate_fare(distance):
    base_fare = 3.0
    per_km_rate = 1.8  # Slightly higher for demo
    return round(base_fare + distance * per_km_rate, 2)

def match_ride(ride):
    def txn(cur):
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
        best = min(candidates, key=lambda row: _haversine(px, py, row[1], row[2]))
        driver_id = best[0]

        cur.execute("SELECT status FROM drivers WHERE driver_id = %s FOR UPDATE;", (driver_id,))
        if cur.fetchone()[0] != "AVAILABLE":
            raise Exception("Driver locked by another transaction")

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

        cur.execute("""
            UPDATE drivers
            SET status='EN_ROUTE', current_lon=%s, current_lat=%s, last_updated=NOW()
            WHERE driver_id=%s;
        """, (ride["pickup_lon"], ride["pickup_lat"], driver_id))

        return driver_id
    return run_txn(txn)

def complete_ride(ride, driver_id):
    def txn(cur):
        distance = _haversine(
            ride["pickup_lon"], ride["pickup_lat"],
            ride["dropoff_lon"], ride["dropoff_lat"]
        )
        fare = calculate_fare(distance)

        cur.execute("""
            INSERT INTO trips_p (
                ride_id, driver_id, start_time, end_time, total_amount, distance
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ride_id) DO NOTHING;
        """, (
            ride["ride_id"], driver_id,
            ride["pickup_datetime"],
            ride["dropoff_datetime"],
            fare, distance
        ))

        cur.execute("UPDATE rides_p SET status='COMPLETED' WHERE ride_id=%s;", (ride["ride_id"],))

        cur.execute("""
            UPDATE drivers
            SET status='AVAILABLE', current_lon=%s, current_lat=%s, last_updated=NOW()
            WHERE driver_id=%s;
        """, (ride["dropoff_lon"], ride["dropoff_lat"], driver_id))

    run_txn(txn)
