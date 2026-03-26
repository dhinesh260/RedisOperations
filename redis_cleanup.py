#!/usr/bin/env python3
"""
Redis Cleanup Script — TTL-based age detection
===============================================
Keys are created with a fixed TTL (default 7 days / 604800s).
Age is back-calculated as:

    age = original_ttl - remaining_ttl

Any key older than the configured keep window is deleted.
Keys with no TTL (persistent) are left untouched.

Usage examples:
    python redis_cleanup.py                        # keep last 1 day
    python redis_cleanup.py --keep-days 3          # keep last 3 days
    python redis_cleanup.py --keep-days 3 --dry-run
    python redis_cleanup.py --host 10.0.0.1 --port 6380 --password secret
    python redis_cleanup.py --databases 0 1        # only clean DB 0 and 1
    python redis_cleanup.py --original-ttl 86400   # if your TTL is 1 day
"""

import argparse
import logging

import redis

# ---------------------------------------------------------------------------
# Defaults  (edit here or override via CLI flags)
# ---------------------------------------------------------------------------
DEFAULT_HOST         = "localhost"
DEFAULT_PORT         = 6379
DEFAULT_PASSWORD     = None          # e.g. "mypassword"
DEFAULT_DATABASES    = [0, 1, 2, 3]
DEFAULT_KEEP_DAYS    = 1             # how many days of data to KEEP
DEFAULT_ORIGINAL_TTL = 604800        # 7 days — TTL set at key creation (seconds)
SCAN_BATCH_SIZE      = 500           # keys per SCAN call
DELETE_BATCH_SIZE    = 1000          # keys per DEL call

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def is_stale(remaining_ttl: int, original_ttl: int, cutoff_age_seconds: float) -> bool:
    """
    Return True if the key is older than the cutoff and should be deleted.

    remaining_ttl      — seconds until the key expires (from Redis TTL command)
    original_ttl       — seconds the key was given at creation
    cutoff_age_seconds — keys >= this many seconds old should be deleted

    age = original_ttl - remaining_ttl

    Example (original_ttl=7 days=604800s, keep_days=1, cutoff=86400s):
        remaining_ttl=561600s → age=43200s  (12 hrs old)  → KEEP
        remaining_ttl=518400s → age=86400s  (1 day old)   → DELETE  (at boundary)
        remaining_ttl=432000s → age=172800s (2 days old)  → DELETE
        remaining_ttl=86400s  → age=518400s (6 days old)  → DELETE

    Safety: if remaining_ttl > original_ttl (TTL was extended after creation),
    age would be negative — we treat such keys as recent and keep them.
    """
    age_seconds = original_ttl - remaining_ttl

    # Guard: negative age means TTL was extended/reset — treat as recent, keep it
    if age_seconds < 0:
        return False

    # Delete if age is AT or BEYOND the cutoff (>=)
    return age_seconds >= cutoff_age_seconds


def cleanup_db(
    host: str,
    port: int,
    db: int,
    password: str | None,
    keep_days: int,
    original_ttl: int,
    dry_run: bool,
) -> dict:
    """Scan one Redis DB, find stale keys, delete them. Returns stats."""

    stats = {
        "db": db,
        "scanned": 0,
        "deleted": 0,
        "skipped_no_ttl": 0,    # persistent keys — left untouched
        "skipped_recent": 0,    # within the keep window
        "errors": 0,
    }

    # --- connect ---
    try:
        client = redis.Redis(
            host=host, port=port, db=db, password=password,
            decode_responses=True, socket_connect_timeout=5,
        )
        client.ping()
    except redis.ConnectionError as exc:
        log.error("DB %d — cannot connect: %s", db, exc)
        stats["errors"] += 1
        return stats

    cutoff_age_seconds = keep_days * 86400   # e.g. 1 day = 86400 s

    log.info(
        "DB %d — scanning | keep_days=%d | original_ttl=%ds | dry_run=%s",
        db, keep_days, original_ttl, dry_run,
    )

    stale_keys: list[str] = []
    cursor = 0

    while True:
        cursor, keys = client.scan(cursor=cursor, count=SCAN_BATCH_SIZE)
        stats["scanned"] += len(keys)

        if keys:
            # Fetch all TTLs in a single pipeline round-trip (no extra RTT per key)
            pipe = client.pipeline(transaction=False)
            for key in keys:
                pipe.ttl(key)
            ttls = pipe.execute()

            for key, ttl in zip(keys, ttls):
                if ttl == -1:
                    # No TTL set — persistent key, skip as configured
                    stats["skipped_no_ttl"] += 1
                    log.debug("DB %d | SKIP (no TTL)        → %s", db, key)

                elif ttl == -2:
                    # Key vanished between SCAN and TTL (already expired) — ignore
                    log.debug("DB %d | SKIP (already gone)  → %s", db, key)

                elif is_stale(ttl, original_ttl, cutoff_age_seconds):
                    stale_keys.append(key)
                    age_hours = (original_ttl - ttl) / 3600
                    log.debug("DB %d | STALE  (age=%.1fh)   → %s", db, age_hours, key)

                else:
                    stats["skipped_recent"] += 1
                    log.debug("DB %d | RECENT (ttl=%ds)     → %s", db, ttl, key)

        if cursor == 0:
            break

    # --- delete (or preview) ---
    if not stale_keys:
        log.info("DB %d — nothing to delete.", db)
        return stats

    if dry_run:
        stats["deleted"] = len(stale_keys)
        log.info("DB %d — [DRY RUN] would delete %d key(s):", db, len(stale_keys))
        for k in stale_keys[:20]:
            log.info("    → %s", k)
        if len(stale_keys) > 20:
            log.info("    … and %d more", len(stale_keys) - 20)
    else:
        for i in range(0, len(stale_keys), DELETE_BATCH_SIZE):
            batch = stale_keys[i : i + DELETE_BATCH_SIZE]
            try:
                deleted = client.delete(*batch)
                stats["deleted"] += deleted
            except redis.RedisError as exc:
                log.error("DB %d — delete error: %s", db, exc)
                stats["errors"] += 1

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete Redis keys older than N days from specified databases.\n\n"
            "Age is derived from:  age = original_ttl - remaining_ttl\n"
            "Persistent keys (no TTL) are always skipped.\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python redis_cleanup.py                          # keep last 1 day\n"
            "  python redis_cleanup.py --keep-days 3            # keep last 3 days\n"
            "  python redis_cleanup.py --keep-days 1 --dry-run  # preview only\n"
            "  python redis_cleanup.py --databases 0 2 3        # specific DBs\n"
        ),
    )
    parser.add_argument(
        "--host", default=DEFAULT_HOST,
        help=f"Redis host (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port", default=DEFAULT_PORT, type=int,
        help=f"Redis port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--password", default=DEFAULT_PASSWORD,
        help="Redis password (optional)",
    )
    parser.add_argument(
        "--databases", default=DEFAULT_DATABASES, type=int, nargs="+", metavar="DB",
        help=f"DB numbers to clean (default: {DEFAULT_DATABASES})",
    )
    parser.add_argument(
        "--keep-days", default=DEFAULT_KEEP_DAYS, type=int,
        help=f"Days of recent data to KEEP (default: {DEFAULT_KEEP_DAYS})",
    )
    parser.add_argument(
        "--original-ttl", default=DEFAULT_ORIGINAL_TTL, type=int,
        help=(
            f"TTL in seconds that was set at key creation "
            f"(default: {DEFAULT_ORIGINAL_TTL} = 7 days)"
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview deletions without actually removing keys",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable DEBUG logging (prints every key decision)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Sanity checks
    if args.keep_days < 1:
        log.error("--keep-days must be >= 1")
        raise SystemExit(1)

    cutoff_seconds = args.keep_days * 86400
    if cutoff_seconds >= args.original_ttl:
        log.warning(
            "--keep-days (%d days = %ds) is >= --original-ttl (%ds). "
            "No keys would qualify for deletion — check your configuration.",
            args.keep_days, cutoff_seconds, args.original_ttl,
        )

    log.info("=" * 60)
    log.info("Redis Cleanup")
    log.info("  Host         : %s:%d", args.host, args.port)
    log.info("  Databases    : %s", args.databases)
    log.info("  Keep days    : %d", args.keep_days)
    log.info("  Original TTL : %ds  (%.1f days)", args.original_ttl, args.original_ttl / 86400)
    log.info("  Dry run      : %s", args.dry_run)
    log.info("=" * 60)

    totals = {"scanned": 0, "deleted": 0, "skipped_no_ttl": 0, "skipped_recent": 0, "errors": 0}

    for db in args.databases:
        stats = cleanup_db(
            host=args.host,
            port=args.port,
            db=db,
            password=args.password,
            keep_days=args.keep_days,
            original_ttl=args.original_ttl,
            dry_run=args.dry_run,
        )
        action = "Would delete" if args.dry_run else "Deleted"
        log.info(
            "DB %d  →  scanned: %d | %s: %d | recent: %d | no-TTL (skipped): %d | errors: %d",
            db,
            stats["scanned"],
            action,
            stats["deleted"],
            stats["skipped_recent"],
            stats["skipped_no_ttl"],
            stats["errors"],
        )
        for k in totals:
            totals[k] += stats[k]

    log.info("-" * 60)
    log.info(
        "TOTAL  →  scanned: %d | %s: %d | recent: %d | no-TTL (skipped): %d | errors: %d",
        totals["scanned"],
        "Would delete" if args.dry_run else "Deleted",
        totals["deleted"],
        totals["skipped_recent"],
        totals["skipped_no_ttl"],
        totals["errors"],
    )
    log.info("Done.")


if __name__ == "__main__":
    main()
