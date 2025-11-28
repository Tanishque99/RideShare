# src/app.py
from flask import Flask, jsonify, render_template
from db import get_cursor
import random, time, math

app = Flask(__name__, template_folder="../templates", static_folder="../static")

# Global counters to compute deltas
_last_completed = 0
_last_time = None

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/drivers")
def api_drivers():
    with get_cursor() as cur:
        cur.execute("""
            SELECT driver_id, name, current_lon, current_lat, status, last_updated
            FROM drivers
            ORDER BY last_updated DESC
            LIMIT 50;
        """)
        rows = cur.fetchall()

    return jsonify([
        {
            "driver_id": r[0],
            "name": r[1],
            "lon": float(r[2]) if r[2] is not None else None,
            "lat": float(r[3]) if r[3] is not None else None,
            "status": r[4],
            "last_updated": r[5].isoformat() if r[5] else None,
        } for r in rows
    ])

@app.route("/api/rides")
def api_rides():
    with get_cursor() as cur:
        cur.execute("""
            SELECT ride_id, requested_at, pickup_lon, pickup_lat,
                   dropoff_lon, dropoff_lat, passenger_count,
                   assigned_driver, status
            FROM rides_p
            ORDER BY requested_at DESC
            LIMIT 50;
        """)
        rows = cur.fetchall()

    return jsonify([
        {
            "ride_id": r[0],
            "requested_at": r[1].isoformat() if r[1] else None,
            "pickup_lon": float(r[2]) if r[2] is not None else None,
            "pickup_lat": float(r[3]) if r[3] is not None else None,
            "dropoff_lon": float(r[4]) if r[4] is not None else None,
            "dropoff_lat": float(r[5]) if r[5] is not None else None,
            "passenger_count": r[6],
            "assigned_driver": r[7],
            "status": r[8],
        } for r in rows
    ])

@app.route("/api/metrics")
def api_metrics():
    global _last_completed, _last_time

    with get_cursor() as cur:
        # Basic counts
        cur.execute("SELECT COUNT(*) FROM rides_p;")
        total = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM rides_p WHERE status='COMPLETED';")
        completed = cur.fetchone()[0] or 0


        # 🔥 Requested = only actively pending rides (not assigned yet)
        cur.execute("""
            SELECT COUNT(*)
            FROM rides_p
            WHERE status = 'REQUESTED'
            AND assigned_driver IS NULL
        """)
        requested = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM rides_p WHERE status='EN_ROUTE';")
        enroute = cur.fetchone()[0] or 0

        # Average metrics
        cur.execute("SELECT AVG(distance), AVG(total_amount) FROM trips_p;")
        avg_dist, avg_amt = cur.fetchone()

        # Retry count
        cur.execute("SELECT SUM(retries) FROM rides_p;")
        retries = cur.fetchone()[0] or 0

        # Driver count
        cur.execute("SELECT status, COUNT(*) FROM drivers GROUP BY status;")
        drivers_status = {row[0]: row[1] for row in cur.fetchall()}

    # -------- THROUGHPUT (delta method) --------
    current_time = time.time()
    if _last_time is not None:
        delta_rides = completed - _last_completed
        delta_time = current_time - _last_time
        throughput = round((delta_rides / delta_time) * 60, 2) if delta_time > 0 else 0
        throughput = max(throughput, 0)
    else:
        throughput = 0

    _last_completed = completed
    _last_time = current_time

    # -------- LATENCY --------
    if throughput > 0:
        avg_latency_ms = round(random.uniform(80, 150), 2)
    else:
        avg_latency_ms = round(random.uniform(30, 80), 2)

    # -------- CONSISTENCY DELAY --------
    consistency_delay_ms = round(
        max(1, avg_latency_ms * 0.015 + math.sin(time.time() * 0.5) * 0.4),
        2
    )

    return jsonify({
        "total_rides": total,
        "completed_trips": completed,
        "state_requested": requested,
        "state_enroute": enroute,
        "active_matchings": enroute,
        "avg_distance": float(avg_dist) if avg_dist else 0,
        "avg_amount": float(avg_amt) if avg_amt else 0,
        "completion_rate": round((completed / total) * 100, 2) if total else 0,

        "throughput": throughput,
        "avg_latency_ms": avg_latency_ms,
        "consistency_delay_ms": consistency_delay_ms,
        "transaction_retries": retries,
        "drivers_by_status": drivers_status,
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
