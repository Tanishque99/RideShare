# src/init_drivers.py
import random
from db import get_cursor
import redis

# Redis setup (only if caching is used)
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
BUSY_KEY = "busy_drivers"


def init_drivers(n=10, clear_existing=False):
    """
    Initialize n drivers distributed across NYC metro region.
    Matches loader.py region scale.
    
    Parameters:
        n (int): number of drivers
        clear_existing (bool): if True → wipe existing drivers
    """
    print(f"[init_drivers] Initializing {n} drivers (wide distribution)...")

    NYC_MIN_LON, NYC_MAX_LON = -74.25, -73.75
    NYC_MIN_LAT, NYC_MAX_LAT = 40.40, 40.80

    with get_cursor(commit=True) as cur:
        if clear_existing:
            print("[init_drivers] 🧹 Clearing existing driver records...")
            cur.execute("DELETE FROM drivers;")
            # Also clear busy cache
            r.delete(BUSY_KEY)

        for i in range(n):
            lon = NYC_MIN_LON + random.random() * (NYC_MAX_LON - NYC_MIN_LON)
            lat = NYC_MIN_LAT + random.random() * (NYC_MAX_LAT - NYC_MIN_LAT)

            lon += (random.random() - 0.5) * 0.02
            lat += (random.random() - 0.5) * 0.02

            cur.execute("""
                INSERT INTO drivers (
                    name, current_lon, current_lat, status, last_updated
                )
                VALUES (%s, %s, %s, 'AVAILABLE', NOW())
                ON CONFLICT (driver_id) DO NOTHING;
            """, (f"Driver_{i+1}", lon, lat))

    print(f"[init_drivers] ✔ {n} drivers created. Redis cache cleared = {clear_existing}")


if __name__ == "__main__":
    # For repeated test runs: use clear_existing=True
    init_drivers(10, clear_existing=True)
