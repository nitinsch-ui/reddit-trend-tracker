"""Runs the layer-4 SQL checks in dq_scheduled.sql on a fixed interval."""

from __future__ import annotations

import logging
import os
import re
import time
from pathlib import Path

log = logging.getLogger("dq-scheduler")

BLOCK = re.compile(r"^-- (check|maintenance): (\w+)\s*$", re.MULTILINE)


def parse_blocks(sql: str) -> list[tuple[str, str, str]]:
    """Split the file into (kind, name, sql) blocks."""
    markers = list(BLOCK.finditer(sql))
    blocks = []
    for i, m in enumerate(markers):
        end = markers[i + 1].start() if i + 1 < len(markers) else len(sql)
        blocks.append((m.group(1), m.group(2), sql[m.end():end].strip()))
    return blocks


def run_once(conn, blocks) -> None:
    with conn.cursor() as cur:
        for kind, name, sql in blocks:
            try:
                cur.execute(sql)
                if kind == "maintenance":
                    log.info("maintenance %s done", name)
                    continue
                status, value, threshold, details = cur.fetchone()
                cur.execute(
                    "INSERT INTO dq_results (layer, check_name, status, metric_value, threshold, details) "
                    "VALUES ('scheduled_sql', %s, %s, %s, %s, %s)",
                    (name, status, float(value), float(threshold), details),
                )
                log.log(logging.INFO if status == "pass" else logging.WARNING, "%s: %s (%s)", name, status, value)
            except Exception:  # noqa: BLE001 - one broken check must not stop the others
                log.exception("check %s failed to run", name)


def main() -> None:
    import psycopg

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    blocks = parse_blocks(Path(os.getenv("DQ_SQL_PATH", "/app/dq_scheduled.sql")).read_text(encoding="utf-8"))
    interval = int(os.getenv("DQ_INTERVAL_SECONDS", "300"))
    dsn = (f"host={os.environ['POSTGRES_HOST']} port={os.getenv('POSTGRES_PORT', '5432')} "
           f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} "
           f"password={os.environ['POSTGRES_PASSWORD']}")
    log.info("running %d blocks every %ss", len(blocks), interval)
    while True:
        try:
            with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
                run_once(conn, blocks)
        except Exception:  # noqa: BLE001
            log.exception("scheduled DQ run failed")
        time.sleep(interval)


if __name__ == "__main__":
    main()
