from loader import load_synthetic
from cleaner import clean_data
from init_drivers import init_drivers
from replayer import replayer
import threading
import time

def main():
    load_synthetic(500000)
    clean_data()
    init_drivers(25000)
    replay_thread = threading.Thread(target=replayer, args=(5000,), daemon=True)
    replay_thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
