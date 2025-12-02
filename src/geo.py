# src/geo.py
"""
Simple region bucketing for NYC coordinates.

We partition into 4 regions:
  0 = North-West
  1 = North-East
  2 = South-West
  3 = South-East
"""

LON_SPLIT = -73.9   # vertical line
LAT_SPLIT = 40.65   # horizontal line

def get_region(lon: float, lat: float) -> int:
    if lon is None or lat is None:
        return 0  # default / unknown

    if lat >= LAT_SPLIT:
        # North
        return 0 if lon <= LON_SPLIT else 1
    else:
        # South
        return 2 if lon <= LON_SPLIT else 3
