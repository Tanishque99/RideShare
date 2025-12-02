# src/init_drivers.py
from db import get_cursor
import random

def init_drivers(n=10):
    """
    Initialize drivers based on random pickup locations from nyc_clean.
    """
    print(f"[init_drivers] Initializing {n} drivers...")

    coords = []
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT pickup_lon, pickup_lat
            FROM nyc_clean
            LIMIT %s;
        """, (n * 5,))
        coords = cur.fetchall()

    if len(coords) < n:
        print(f"[init_drivers] Warning: only {len(coords)} coordinates available.")

    sample = random.sample(coords, min(n, len(coords)))

    with get_cursor(commit=True) as cur:
        for i, (lon, lat) in enumerate(sample):
            cur.execute("""
                INSERT INTO drivers (name, current_lon, current_lat, status)
                VALUES (%s, %s, %s, 'AVAILABLE')
            """, (f"Driver_{i+1}", lon, lat))

    print("[init_drivers] Done.")


if __name__ == "__main__":
    init_drivers(10000)
