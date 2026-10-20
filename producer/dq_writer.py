"""Writes producer validation counts to the shared dq_results table."""

from __future__ import annotations

import logging

from validator import CHECK_NAMES

log = logging.getLogger(__name__)


def cycle_rows(failures: dict[str, int], total: int) -> list[tuple]:
    """One row per layer-1 check: (layer, check, status, failures, threshold, details)."""
    rows = []
    for check in CHECK_NAMES:
        failed = failures.get(check, 0)
        rows.append(("producer", check, "pass" if failed == 0 else "fail", float(failed), 0.0,
                     f"{failed} of {total} posts failed in this poll cycle"))
    return rows


class DqWriter:
    """Best-effort: a Postgres outage must never stop ingestion."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn = None

    def _connection(self):
        import psycopg

        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=5)
        return self._conn

    def write_cycle(self, failures: dict[str, int], total: int) -> None:
        if total == 0:
            return
        try:
            with self._connection().cursor() as cur:
                cur.executemany(
                    "INSERT INTO dq_results (layer, check_name, status, metric_value, threshold, details) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    cycle_rows(failures, total),
                )
        except Exception as exc:  # noqa: BLE001 - deliberately broad, see class docstring
            log.warning("could not write dq_results: %s", exc)
            self._conn = None
