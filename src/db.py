# src/db.py
import psycopg2
from psycopg2 import Error as PsycopgError  # catch all DB errors
from contextlib import contextmanager
import time

# Connect to your 3-node Cockroach cluster via node 1 (localhost:26257)
DB_DSN = "postgresql://tanishque:mU68qegClXd_TarenFLWIQ@mythic-scylla-19024.j77.aws-us-west-2.cockroachlabs.cloud:26257/rideshare?sslmode=verify-full"


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


def run_txn(fn, max_retries=10):
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
                # NEW: record retry for metrics if ride_id is available
                try:
                    # pass ride_id through thread-local for metrics (set in match_ride)
                    from matcher import current_retry_ride_id  
                    if current_retry_ride_id is not None:
                        with get_cursor(commit=True) as mcur:
                            mcur.execute("""
                                UPDATE rides_p
                                SET retries = COALESCE(retries, 0) + 1
                                WHERE ride_id = %s;
                            """, (current_retry_ride_id,))
                except Exception as m_err:
                    print("retry metric error:", m_err)

                # existing logic
                try:
                    conn.rollback()
                except Exception:
                    pass

                sleep = 0.1 * (2 ** attempt)
                print(f"[run_txn] Retryable Cockroach error (40001) attempt {attempt+1}/{max_retries}, sleeping {sleep:.2f}s")
                time.sleep(sleep)
                continue


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
