# src/run_demo.py
from loader import load_synthetic
from cleaner import clean_data
from init_drivers import init_drivers
from replayer import replayer, process_ride
import threading
import time

def main():
    load_synthetic(1000)
    clean_data()
    init_drivers(100)

    replay_thread = threading.Thread(target=replayer, args=(None,), daemon=True)
    queue_thread = threading.Thread(target=process_ride, daemon=True)

    replay_thread.start()
    queue_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
