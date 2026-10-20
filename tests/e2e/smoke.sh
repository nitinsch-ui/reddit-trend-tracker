#!/usr/bin/env bash
# End-to-end smoke test: posts must flow producer -> Kafka -> Job 1 -> Delta -> Job 2 -> Postgres.
set -euo pipefail

TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"
sql() { docker compose exec -T postgres sh -c "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -tAc \"$1\""; }

deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
until [ "$(sql 'SELECT COUNT(*) FROM topic_trends' 2>/dev/null || echo 0)" -gt 0 ]; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "FAIL: no rows in topic_trends after ${TIMEOUT_SECONDS}s"
    exit 1
  fi
  echo "waiting for trend scores... raw batches=$(sql "SELECT COUNT(*) FROM pipeline_metrics WHERE query_name='raw_writer'" 2>/dev/null || echo '?')"
  sleep 20
done

echo "== topic_trends rows: $(sql 'SELECT COUNT(*) FROM topic_trends')"
echo "== top topics"
sql "SELECT topic, subreddit, ROUND(MAX(trend_score)::numeric, 2), SUM(post_count) FROM topic_trends GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10"
echo "== data-quality checks (latest per check)"
sql "SELECT DISTINCT ON (layer, check_name) layer, check_name, status FROM dq_results ORDER BY layer, check_name, checked_at DESC"
echo "== dead letters"
sql "SELECT stage, error_reason, COUNT(*) FROM dead_letter_events GROUP BY 1, 2"

failed=$(sql "SELECT COUNT(*) FROM (SELECT DISTINCT ON (layer, check_name) status FROM dq_results WHERE layer IN ('spark_job1', 'spark_job2') ORDER BY layer, check_name, checked_at DESC) s WHERE status = 'fail'")
if [ "$failed" -gt 0 ]; then
  echo "FAIL: $failed Spark data-quality checks failing"
  exit 1
fi
echo "== grafana provisioning"
GRAFANA_AUTH="admin:${GRAFANA_ADMIN_PASSWORD:-$(grep -E '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2)}"
curl -sf -u "$GRAFANA_AUTH" http://localhost:3000/api/datasources/uid/postgres-ds/health
echo
curl -sf -u "$GRAFANA_AUTH" http://localhost:3000/api/dashboards/uid/reddit-trends > /dev/null && echo "dashboard provisioned"
rules=$(curl -sf -u "$GRAFANA_AUTH" http://localhost:3000/api/v1/provisioning/alert-rules | python3 -c 'import json, sys; print(len(json.load(sys.stdin)))')
echo "alert rules: $rules"
[ "$rules" -eq 4 ] || { echo "FAIL: expected 4 alert rules"; exit 1; }
echo "PASS"
