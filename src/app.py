# src/app.py
from flask import Flask, jsonify, render_template
from db import get_cursor, run_txn

app = Flask(__name__, template_folder="../templates", static_folder="../static")


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

    drivers = [
        {
            "driver_id": r[0],
            "name": r[1],
            "lon": r[2],
            "lat": r[3],
            "status": r[4],
            "last_updated": r[5].isoformat() if r[5] else None,
        }
        for r in rows
    ]
    return jsonify(drivers)


@app.route("/api/rides")
def api_rides():
    with get_cursor() as cur:
        cur.execute("""
            SELECT
              ride_id,
              requested_at,
              pickup_lon,
              pickup_lat,
              dropoff_lon,
              dropoff_lat,
              passenger_count,
              assigned_driver,
              status
            FROM rides_p
            ORDER BY requested_at DESC
            LIMIT 50;
        """)
        rows = cur.fetchall()

    rides = [
        {
            "ride_id": r[0],
            "requested_at": r[1].isoformat() if r[1] else None,
            "pickup_lon": r[2],
            "pickup_lat": r[3],
            "dropoff_lon": r[4],
            "dropoff_lat": r[5],
            "passenger_count": r[6],
            "assigned_driver": r[7],
            "status": r[8],
        }
        for r in rows
    ]
    return jsonify(rides)


@app.route("/api/metrics")
def api_metrics():
    with get_cursor() as cur:
        cur.execute("SELECT count(*) FROM rides_p;")
        total_rides = cur.fetchone()[0]

        cur.execute("SELECT count(*) FROM trips_p;")
        completed = cur.fetchone()[0]

        cur.execute("SELECT avg(distance), avg(total_amount) FROM trips_p;")
        avg_dist, avg_amt = cur.fetchone()

        cur.execute("""
            SELECT status, count(*) FROM drivers
            GROUP BY status;
        """)
        status_counts = {row[0]: row[1] for row in cur.fetchall()}

    return jsonify({
        "total_rides": total_rides,
        "completed_trips": completed,
        "avg_distance": float(avg_dist) if avg_dist is not None else None,
        "avg_amount": float(avg_amt) if avg_amt is not None else None,
        "drivers_by_status": status_counts,
    })


if __name__ == "__main__":
    app.run(debug=True)
