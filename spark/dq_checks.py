"""Batch-level data-quality checks for the Spark jobs (layers 2 and 3 of 4).

Layer 2 (Job 1): parse_failure_rate, duplicate_rate, nlp_failure_rate
Layer 3 (Job 2): negative_scores, window_integrity
"""

from __future__ import annotations

from typing import NamedTuple, Optional


class CheckResult(NamedTuple):
    layer: str
    check_name: str
    status: str  # pass | warn | fail
    metric_value: float
    threshold: float
    details: str


def rate_check(layer: str, name: str, failures: int, total: int, threshold: float,
               severity: str = "fail") -> Optional[CheckResult]:
    """Fails (or warns) when failures / total exceeds threshold. None for an empty batch."""
    if total == 0:
        return None
    rate = failures / total
    status = "pass" if rate <= threshold else severity
    return CheckResult(layer, name, status, round(rate, 4), threshold, f"{failures} of {total} rows")


def zero_check(layer: str, name: str, bad_rows: int, total: int) -> Optional[CheckResult]:
    """Any bad row fails the check."""
    if total == 0:
        return None
    status = "pass" if bad_rows == 0 else "fail"
    return CheckResult(layer, name, status, float(bad_rows), 0.0, f"{bad_rows} of {total} rows violate the rule")
