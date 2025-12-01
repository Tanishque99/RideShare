# src/app.py
from flask import Flask, jsonify, render_template
from db import get_cursor
import time, math
import redis
import logging

# 🔹 Setup Flask
app = Flask(__name__, template_folder="../templates", static_folder="../static")

# 🔹 Redis client
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# 🔹 Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


# ================================
#  INDEX PAGE
# ================================
@app.route("/")
def index():
    return render_template("index.html")


# ================================
#  GET DRIVERS
# ================================
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


# ================================
#  GET RIDES
# ================================
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


# ================================
#  METRICS (UPDATED)
# ================================
@app.route("/api/metrics")
def api_metrics():
    # Fetch stored values from Redis
    last_completed = int(redis_client.get("metrics:last_completed") or 0)
    last_time = float(redis_client.get("metrics:last_time") or time.time())
    current_time = time.time()

    with get_cursor() as cur:
        # 🎯 Ride metrics
        cur.execute("SELECT COUNT(*) FROM rides_p;")
        total = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM rides_p WHERE status='COMPLETED';")
        completed = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM rides_p WHERE status='FAILED';")
        failed = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT COUNT(*)
            FROM rides_p
            WHERE status='REQUESTED'
            AND assigned_driver IS NULL
        """)
        requested = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM rides_p WHERE status='ASSIGNED';")
        assigned = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM rides_p WHERE status='EN_ROUTE';")
        enroute = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT AVG(distance), AVG(total_amount),
                   AVG(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000)
            FROM trips_p;
        """)
        avg_dist, avg_amt, avg_latency_ms = cur.fetchone()

        cur.execute("SELECT SUM(retries) FROM rides_p;")
        retries = cur.fetchone()[0] or 0

        # 👥 Driver status breakdown
        cur.execute("SELECT status, COUNT(*) FROM drivers GROUP BY status;")
        drivers_status = {row[0]: row[1] for row in cur.fetchall()}

    # 🔥 Derived metrics
    delta_time = max(current_time - last_time, 0.001)
    delta_rides = completed - last_completed
    throughput = round((delta_rides / delta_time) * 60, 2)

    avg_latency_ms = round(avg_latency_ms or 0, 2)
    consistency_delay_ms = round(avg_latency_ms * 0.022, 2)

    # Save updated values to Redis
    redis_client.set("metrics:last_completed", completed)
    if completed > last_completed:
        redis_client.set("metrics:last_time", current_time)

    return jsonify({
        "total_rides": total,
        "completed_trips": completed,
        "state_failed": failed,
        "state_requested": requested,
        "state_assigned": assigned,
        "state_enroute": enroute,
        "active_matchings": assigned + enroute,
        "avg_distance": float(avg_dist) if avg_dist else 0,
        "avg_amount": float(avg_amt) if avg_amt else 0,
        "completion_rate": round((completed / total) * 100, 2) if total else 0,
        "throughput": throughput,
        "avg_latency_ms": avg_latency_ms,
        "consistency_delay_ms": consistency_delay_ms,
        "transaction_retries": retries,
        "drivers_by_status": drivers_status,
    })


# ================================
#  INTERNAL QUERY HELPER
# ================================
def _scalar(sql: str, default=0):
    try:
        with get_cursor() as cur:
            cur.execute(sql)
            v = cur.fetchone()[0]
            return default if v is None else v
    except Exception:
        return default


# ================================
#  CRDB OVERVIEW
# ================================
@app.route("/api/crdb/overview")
def api_crdb_overview():
    total_nodes = int(_scalar("SELECT count(*) FROM crdb_internal.gossip_nodes;", 0))
    live_nodes = int(_scalar("SELECT count(*) FROM crdb_internal.gossip_nodes WHERE is_live;", 0))
    dead_nodes = max(total_nodes - live_nodes, 0)

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

    total_ranges = int(_scalar("SELECT count(*) FROM crdb_internal.ranges;", 0))
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
            "suspect": 0,
            "draining": draining_nodes,
            "dead": dead_nodes,
        },
        "replication": {
            "total_ranges": total_ranges,
            "under_replicated_ranges": under_replicated,
            "unavailable_ranges": unavailable,
        }
    })


# ================================
#  START SERVER
# ================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
