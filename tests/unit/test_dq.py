"""Data-quality layer tests, including the "12 checks across 4 layers" claim."""

import re
from pathlib import Path

from dq_checks import rate_check, zero_check
from run_scheduled import parse_blocks
from validator import CHECK_NAMES

ROOT = Path(__file__).resolve().parents[2]


def test_rate_check():
    assert rate_check("l", "c", 0, 0, 0.1) is None
    assert rate_check("l", "c", 1, 100, 0.05).status == "pass"
    assert rate_check("l", "c", 10, 100, 0.05).status == "fail"
    assert rate_check("l", "c", 10, 100, 0.05, severity="warn").status == "warn"


def test_zero_check():
    assert zero_check("l", "c", 0, 5).status == "pass"
    assert zero_check("l", "c", 1, 5).status == "fail"
    assert zero_check("l", "c", 0, 0) is None


def test_scheduled_sql_blocks():
    blocks = parse_blocks((ROOT / "dq" / "dq_scheduled.sql").read_text())
    kinds = [(k, n) for k, n, _ in blocks]
    assert kinds == [("check", "trend_freshness"), ("check", "trending_topics_present"), ("maintenance", "retention")]
    assert all(sql.strip().endswith(";") for _, _, sql in blocks)


def _spark_check_names():
    pattern = re.compile(r'(?:rate_check|zero_check)\(LAYER, "(\w+)"')
    names = set()
    for job in ("job1_ingest.py", "job2_trend.py"):
        names |= set(pattern.findall((ROOT / "spark" / job).read_text()))
    return names


def test_twelve_checks_across_four_layers():
    spark = _spark_check_names()
    scheduled = {n for k, n, _ in parse_blocks((ROOT / "dq" / "dq_scheduled.sql").read_text()) if k == "check"}
    assert spark == {"parse_failure_rate", "duplicate_rate", "nlp_failure_rate", "negative_scores", "window_integrity"}
    assert len(CHECK_NAMES) + len(spark) + len(scheduled) == 12
