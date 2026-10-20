"""Static checks on the provisioning files that make up the 'zero manual setup' claim."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_dashboard_has_eight_postgres_panels():
    dash = json.loads((ROOT / "grafana/provisioning/dashboards/reddit_trends.json").read_text())
    assert len(dash["panels"]) == 8
    assert dash["refresh"] == "30s"
    assert all(p["datasource"]["uid"] == "postgres-ds" for p in dash["panels"])
    assert len({p["id"] for p in dash["panels"]}) == 8


def test_four_alert_rules():
    text = (ROOT / "grafana/provisioning/alerting/alerts.yml").read_text()
    assert text.count("      - uid: ") == 4


def test_three_topics_five_partitions():
    script = (ROOT / "kafka-setup/create_topics.sh").read_text()
    creates = [line.split() for line in script.splitlines() if line.startswith("create reddit.")]
    assert {c[1]: int(c[2]) for c in creates} == {
        "reddit.raw.posts": 2, "reddit.processed.posts": 2, "reddit.dead.letter": 1}
