#!/usr/bin/env bash
# Creates the three pipeline topics (5 partitions total). Safe to re-run.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh
DAY_MS=$((24 * 3600 * 1000))

until "$KT" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  echo "waiting for kafka at $BOOTSTRAP"; sleep 2
done

create() {
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 \
    --config retention.ms="$3" --config cleanup.policy=delete
}

# One partition per subreddit: worldnews -> 0, todayilearned -> 1.
create reddit.raw.posts       2 $((7 * DAY_MS))
create reddit.processed.posts 2 $((3 * DAY_MS))
# Single partition keeps dead letters in arrival order for investigation and replay.
create reddit.dead.letter     1 $((30 * DAY_MS))

"$KT" --bootstrap-server "$BOOTSTRAP" --describe --topic 'reddit.*'
