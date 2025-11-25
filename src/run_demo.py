# src/run_demo.py
from loader import load_synthetic
from cleaner import clean_data
from init_drivers import init_drivers
from replayer import replayer

def main():
    # Adjust counts as your laptop can handle
    load_synthetic(100)
    clean_data()
    init_drivers(10)
    replayer(60)

if __name__ == "__main__":
    main()
