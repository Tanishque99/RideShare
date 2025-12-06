# RideShare

Ride-sharing platforms need to handle a constant stream of location updates, ride requests, and state changes (requested, assigned, en route, completed). Many users interact with the system at the same time, and they expect fast responses and no double-booking of drivers. This makes the database a critical part of the system.
Traditional single-node databases or simple master–replica setups can become a bottleneck in this setting. They often struggle with heavy writes on popular areas, higher latency for users in other regions, and limited fault tolerance if the primary node fails. Some systems move to NoSQL or manual sharding to scale, but then they may lose strong consistency or become harder to manage.
Recent work on “distributed SQL” databases, such as CockroachDB, aims to combine the familiarity of SQL with automatic sharding, replication, and strong consistency across nodes. RideShareDB, uses CockroachDB to explore how a distributed database can support a realistic ride-allocation workload. The gap we address is that many examples of distributed databases are very simple; they do not show how to handle geo-based matching, high write concurrency, and changing ride states together in one end-to-end system

- **3-node CockroachDB cluster** (local)
- **Synthetic data**
- **Python** for data pipeline & matching logic
- **Flask + HTML** dashboard for live monitoring

The system:

1. Generates synthetic rides into `staging_nyc_raw`
2. Cleans data into `nyc_clean`
3. Initializes drivers
4. Replays rides, assigning closest available drivers with serializable transactions
5. Completes rides after a delay and frees drivers
6. Analytics visible in UI

---

## 1. Prerequisites

- macOS
- [Homebrew](https://brew.sh/)
- Python 3.9+ (`python3 --version`)
- Git (optional)

### Install CockroachDB
'''brew install cockroachdb/cockroach/cockroach'''

## 2. File Structure
    project-root/
    ├── README.md
    ├── db/
    │   └── schema.sql              # All table definitions for the rideshare DB
    ├── src/
    │   ├── db.py                   # CockroachDB connection + transaction helper
    │   ├── loader.py               # Synthetic data generator -> staging_nyc_raw
    │   ├── cleaner.py              # Cleans raw -> nyc_clean
    │   ├── init_drivers.py         # Creates drivers based on nyc_clean
    │   ├── matcher.py              # Core matching + completion logic
    │   ├── replayer.py             # Replays rides, assigns & completes them
    │   ├── app.py                  # Flask API + web server
    │   └── run_demo.py             # Pipeline / demo runner
    ├── templates/
    │   └── index.html              # Frontend dashboard (drivers, rides, metrics)
    └── venv/                       # Python virtual environment (created when setting up)

  ## 3. Cockroach 3 Node Clusters

  Node 1
      
      cockroach start \
        --insecure \
        --store=node1 \
        --listen-addr=localhost:26257 \
        --http-addr=localhost:8080 \
        --join=localhost:26257,localhost:26258,localhost:26259 \
        --background

   Node 2
      
      cockroach start \
        --insecure \
        --store=node2 \
        --listen-addr=localhost:26258 \
        --http-addr=localhost:8081 \
        --join=localhost:26257,localhost:26258,localhost:26259 \
        --background
    
   Node 3
      
      cockroach start \
        --insecure \
        --store=node3 \
        --listen-addr=localhost:26259 \
        --http-addr=localhost:8082 \
        --join=localhost:26257,localhost:26258,localhost:26259 \
        --background

  Initialise Cluster  : 
        
      cockroach init --insecure --host=localhost:26257

  Create Databse : 
      
      cockroach sql --insecure --host=localhost:26257 -e "CREATE DATABASE rideshare;"

  Upload Schema : 

      cockroach sql --insecure --host=localhost:26257 --database=rideshare < db/schema.sql

  Cluster UI - http://localhost:8080/ or http://localhost:8081 or http://localhost:8082

  ## 4. Executing the Algorithm
  1. First open terminal in Project folder and initialise Clusters and Database as shown above.
  2. Next open another terminal and run app.py for UI
  3. Next open another terminal and run run_demo.py for starting matching of users with riders.
  4. Lastly drivers can be seen assigned and unassigned in UI. 
