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

    # STEP 1: Fetch all necessary metrics from DB
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM rides_p;")
        total = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM rides_p WHERE status='COMPLETED';")
        completed = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT COUNT(*)
            FROM rides_p
            WHERE status = 'REQUESTED'
            AND assigned_driver IS NULL
        """)
        requested = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM rides_p WHERE status='EN_ROUTE';")
        enroute = cur.fetchone()[0] or 0

        cur.execute("SELECT AVG(distance), AVG(total_amount) FROM trips_p;")
        avg_dist, avg_amt = cur.fetchone()

        cur.execute("SELECT SUM(retries) FROM rides_p;")
        retries = cur.fetchone()[0] or 0

        cur.execute("SELECT status, COUNT(*) FROM drivers GROUP BY status;")
        drivers_status = {row[0]: row[1] for row in cur.fetchall()}

    # STEP 2: Detect idle state
    is_idle = (requested == 0 and enroute == 0 and completed == 0)

    if is_idle:
        return jsonify({
            "total_rides": total,
            "completed_trips": completed,
            "state_requested": requested,
            "state_enroute": enroute,
            "active_matchings": enroute,
            "avg_distance": float(avg_dist) if avg_dist else 0,
            "avg_amount": float(avg_amt) if avg_amt else 0,
            "completion_rate": 0,
            "throughput": None,
            "avg_latency_ms": None,
            "consistency_delay_ms": None,
            "transaction_retries": retries,
            "drivers_by_status": drivers_status,
        })

    # STEP 3: Compute throughput
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

    if completed == 0 or total == 0:
        throughput = 0

    # STEP 4: Real latency computation (via match_latency_ms)
    with get_cursor() as cur:
        cur.execute("SELECT AVG(match_latency_ms) FROM rides_p WHERE match_latency_ms IS NOT NULL;")
        avg_latency_db = cur.fetchone()[0]

    avg_latency_ms = round(avg_latency_db, 2) if avg_latency_db is not None else 0
    try:
        with get_cursor() as cur:
            cur.execute("""
                SELECT AVG((metrics->>'consistency.queue.processing.latency')::FLOAT)
                FROM crdb_internal.kv_store_status;
            """)
            consistency_delay_db = cur.fetchone()[0]
        # Convert from seconds → ms
        consistency_delay_ms = round(consistency_delay_db * 1000, 2) if consistency_delay_db is not None else None
    except Exception:
        consistency_delay_ms = None  # Display as "Idle" if not available


    # STEP 5: Return real-time metrics
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

def _scalar(sql: str, default=0):
    try:
        with get_cursor() as cur:
            cur.execute(sql)
            v = cur.fetchone()[0]
            return default if v is None else v
    except Exception:
        return default

@app.route("/api/crdb/overview")
def api_crdb_overview():
    # Node status
    total_nodes = int(_scalar("SELECT count(*) FROM crdb_internal.gossip_nodes;", 0))
    live_nodes  = int(_scalar("SELECT count(*) FROM crdb_internal.gossip_nodes WHERE is_live;", 0))  
    dead_nodes  = max(total_nodes - live_nodes, 0)

    # Draining column name varies by version; detect via information_schema
    draining_nodes = 0
    with get_cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='crdb_internal' AND table_name='node_runtime_info';
        """)
        cols = {r[0] for r in cur.fetchall()}

    if "draining" in cols:
        draining_nodes = int(_scalar("SELECT count(*) FROM crdb_internal.node_runtime_info WHERE draining;", 0))
    elif "is_draining" in cols:
        draining_nodes = int(_scalar("SELECT count(*) FROM crdb_internal.node_runtime_info WHERE is_draining;", 0))

    # Replication status
    total_ranges = int(_scalar("SELECT count(*) FROM crdb_internal.ranges;", 0))  

    # These are Cockroach metrics: ranges.underreplicated / ranges.unavailable 
    under_replicated = int(_scalar("""
        SELECT COALESCE(sum((metrics->>'ranges.underreplicated')::DECIMAL), 0)::INT
        FROM crdb_internal.kv_store_status;
    """, 0))

    unavailable = int(_scalar("""
        SELECT COALESCE(sum((metrics->>'ranges.unavailable')::DECIMAL), 0)::INT
        FROM crdb_internal.kv_store_status;
    """, 0))

    return jsonify({
        "nodes": {
            "total": total_nodes,
            "live": live_nodes,
            "suspect": 0,          # optional: add later if you decide on a definition/source
            "draining": draining_nodes,
            "dead": dead_nodes,
        },
        "replication": {
            "total_ranges": total_ranges,
            "under_replicated_ranges": under_replicated,
            "unavailable_ranges": unavailable,
        }
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
