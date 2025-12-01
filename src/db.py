# src/db.py
import psycopg2
from psycopg2 import Error as PsycopgError  # catch all DB errors
from contextlib import contextmanager
import time

# ===========================================
# DATABASE CONNECTION
# ===========================================
# Connect to your 3-node Cockroach cluster via node 1 (localhost:26257)
DB_DSN = "postgresql://root@localhost:26257/rideshare?sslmode=disable"


def get_conn():
    """
    Open a new connection to CockroachDB.
    Recommended: short-lived connections only.
    """
    return psycopg2.connect(DB_DSN)


# ===========================================
# SIMPLE CURSOR (non-transactional)
# ===========================================
@contextmanager
def get_cursor(commit=False):
    """
    Light-use context manager for simple (single query) operations
    that don't need SERIALIZABLE retries.
    Use run_txn() for multi-statement transactions.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        yield cur
        if commit:
            conn.commit()
    finally:
        conn.close()


# ===========================================
# RETRYABLE TRANSACTION BLOCK
# ===========================================
def run_txn(fn, max_retries=5):
    """
    Execute fn(cur) within a SERIALIZABLE transaction with auto retries
    for transient CockroachDB conflicts (SQLSTATE 40001).

    Parameters:
    - fn: Function that accepts cursor as argument.
    - max_retries: Maximum retry attempts.

    Returns:
        Value returned by fn(cur) upon successful commit.
    """
    for attempt in range(max_retries):
        conn = get_conn()
        try:
            # Cockroach best practice: SERIALIZABLE isolation
            conn.set_session(isolation_level="SERIALIZABLE")
            cur = conn.cursor()

            # Execute client logic
            result = fn(cur)

            # Try to commit
            conn.commit()
            cur.close()
            return result

        except PsycopgError as e:
            code = getattr(e, "pgcode", None)

            # Retryable conflict
            if code == "40001" and attempt < max_retries - 1:
                try:
                    conn.rollback()
                except Exception:
                    pass

                # Exponential backoff (+ jitter)
                sleep_time = 0.1 * (2 ** attempt) + random_jitter()
                print(
                    f"[run_txn] ⚠️ Retryable CockroachDB conflict (40001) "
                    f"on attempt {attempt+1}/{max_retries}, backing off {sleep_time:.2f}s"
                )
                time.sleep(sleep_time)
                continue

            # Final failure or non-retryable error
            try:
                conn.rollback()
            except Exception:
                pass

            print(f"[run_txn] Transaction failed (attempt {attempt+1}): {e}")
            raise  # re-raise so caller handles it

        finally:
            try:
                conn.close()
            except Exception:
                pass


# ===========================================
# EXTRA: ADD SMALL RANDOM JITTER
# ===========================================
# Helps avoid stampede during conflict retries
import random

def random_jitter(max_ms=50):
    """Add small random jitter to avoid synchronized retries."""
    return random.uniform(0, max_ms / 1000.0)

