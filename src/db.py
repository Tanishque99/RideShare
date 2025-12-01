# src/db.py
import psycopg2
from psycopg2 import Error as PsycopgError  # catch all DB errors
from contextlib import contextmanager
import time
import logging

# 🔍 DIAGNOSTIC LOGGING
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(message)s"
)
db_logger = logging.getLogger("db_debug")
db_logger.setLevel(logging.WARNING)  # Reduced logging for large-scale test

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
# 🔍 DIAGNOSTIC: Track retry statistics
_retry_stats = {
    "total_retries": 0,
    "successful_after_retry": 0,
    "failed_after_max_retries": 0,
    "conflicts_by_attempt": {},
}


def get_retry_stats():
    """Return current retry statistics for diagnosis."""
    return _retry_stats.copy()


def reset_retry_stats():
    """Reset retry statistics."""
    global _retry_stats
    _retry_stats = {
        "total_retries": 0,
        "successful_after_retry": 0,
        "failed_after_max_retries": 0,
        "conflicts_by_attempt": {},
    }


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
    txn_start = time.time()
    fn_name = getattr(fn, "__name__", "anonymous")

    for attempt in range(max_retries):
        attempt_start = time.time()
        conn = get_conn()
        try:
            # Cockroach best practice: SERIALIZABLE isolation
            conn.set_session(isolation_level="SERIALIZABLE")
            cur = conn.cursor()

            # Execute client logic
            result = fn(cur)

            # Try to commit
            commit_start = time.time()
            conn.commit()
            commit_time = (time.time() - commit_start) * 1000
            cur.close()

            total_time = (time.time() - txn_start) * 1000

            # 🔍 DIAGNOSTIC: Log successful transaction
            if attempt > 0:
                _retry_stats["successful_after_retry"] += 1
                db_logger.info(
                    f"[DIAG] TXN SUCCESS after {attempt} retries, total_time={total_time:.2f}ms, commit_time={commit_time:.2f}ms"
                )
            else:
                db_logger.debug(
                    f"[DIAG] TXN SUCCESS on first attempt, total_time={total_time:.2f}ms, commit_time={commit_time:.2f}ms"
                )

            return result

        except PsycopgError as e:
            code = getattr(e, "pgcode", None)
            attempt_time = (time.time() - attempt_start) * 1000

            # 🔍 DIAGNOSTIC: Log every conflict
            db_logger.warning(
                f"[DIAG] TXN CONFLICT: pgcode={code}, attempt={attempt+1}/{max_retries}, attempt_time={attempt_time:.2f}ms, error={str(e)[:100]}"
            )

            # Track retry statistics
            _retry_stats["total_retries"] += 1
            attempt_key = str(attempt + 1)
            _retry_stats["conflicts_by_attempt"][attempt_key] = (
                _retry_stats["conflicts_by_attempt"].get(attempt_key, 0) + 1
            )

            # Retryable conflict
            if code == "40001" and attempt < max_retries - 1:
                try:
                    conn.rollback()
                except Exception:
                    pass

                # Exponential backoff (+ jitter)
                sleep_time = 0.1 * (2**attempt) + random_jitter()
                db_logger.info(
                    f"[DIAG] TXN RETRY: Retryable CockroachDB conflict (40001) "
                    f"on attempt {attempt+1}/{max_retries}, backing off {sleep_time:.2f}s"
                )
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

            _retry_stats["failed_after_max_retries"] += 1
            total_time = (time.time() - txn_start) * 1000
            db_logger.error(
                f"[DIAG] TXN FAILED: pgcode={code}, total_attempts={attempt+1}, total_time={total_time:.2f}ms"
            )
            db_logger.error(f"[DIAG] Current retry stats: {_retry_stats}")

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
