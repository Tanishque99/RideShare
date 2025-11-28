# src/db.py
import psycopg2
from psycopg2 import Error as PsycopgError  # catch all DB errors
from contextlib import contextmanager
import time

# Connect to your 3-node Cockroach cluster via node 1 (localhost:26257)
DB_DSN = "postgresql://root@localhost:26257/rideshare?sslmode=disable"


def get_conn():
    return psycopg2.connect(DB_DSN)


@contextmanager
def get_cursor(commit=False):
    """
    Simple context manager for non-transactional queries.
    Use run_txn() for serializable multi-statement transactions.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        yield cur
        if commit:
            conn.commit()
    finally:
        conn.close()


def run_txn(fn, max_retries=5):
    """
    Run fn(cur) inside a SERIALIZABLE transaction with automatic retries
    on CockroachDB retryable errors (SQLSTATE 40001).

    fn should accept a single argument: cursor, and return a value.
    """
    for attempt in range(max_retries):
        conn = get_conn()
        try:
            # Cockroach recommends SERIALIZABLE (default) – set it explicitly.
            conn.set_session(isolation_level="SERIALIZABLE")
            cur = conn.cursor()

            result = fn(cur)
            conn.commit()
            cur.close()
            return result

        except PsycopgError as e:
            code = getattr(e, "pgcode", None)

            # 40001 = serialization_failure → safe to retry
            if code == "40001" and attempt < max_retries - 1:
                try:
                    conn.rollback()
                except Exception:
                    pass

                sleep = 0.1 * (2 ** attempt)  # exponential backoff
                print(
                    f"[run_txn] Retryable Cockroach error (40001) on attempt "
                    f"{attempt+1}/{max_retries}, sleeping {sleep:.2f}s"
                )
                time.sleep(sleep)
                continue  # try again

            # Non-retryable or out of retries → re-raise
            try:
                conn.rollback()
            except Exception:
                pass

            print(f"[run_txn] Non-retryable error or max retries reached: {e}")
            raise

        finally:
            try:
                conn.close()
            except Exception:
                pass
