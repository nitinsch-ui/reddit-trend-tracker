.DEFAULT_GOAL := help
COMPOSE := docker compose
s ?=

.PHONY: help env up down logs ps tools topics psql trends dq dlq backfill test test-spark lint check reset sample convert fetch real

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from .env.example if it does not exist
	@test -f .env || (cp .env.example .env && echo "created .env - edit passwords / Reddit credentials there")

up: env ## Build and start the whole stack
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  Grafana     http://localhost:3000   (dashboard: Reddit Trend Tracker)"
	@echo "  Spark UI    http://localhost:4040 (job 1)  http://localhost:4041 (job 2)"
	@echo "  NLP docs    http://localhost:8000/docs"

down: ## Stop the stack (keeps data volumes)
	$(COMPOSE) down

logs: ## Follow logs (all services, or one: make logs s=spark-job1)
	$(COMPOSE) logs -f --tail=100 $(s)

ps: ## Show service status and health
	$(COMPOSE) ps

tools: ## Start Kafka UI on http://localhost:8080
	$(COMPOSE) --profile tools up -d kafka-ui

topics: ## Describe the Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic 'reddit.*'

psql: ## Open psql on the serving database
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

trends: ## Print the current top 10 trending topics
	@$(COMPOSE) exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT topic, subreddit, ROUND(MAX(trend_score)::numeric, 2) AS peak_score, SUM(post_count) AS posts FROM topic_trends WHERE is_trending AND window_start >= NOW() - INTERVAL '"'"'1 hour'"'"' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10"'

dq: ## Show the latest result of every data-quality check
	@$(COMPOSE) exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT DISTINCT ON (layer, check_name) layer, check_name, status, metric_value, threshold, checked_at FROM dq_results ORDER BY layer, check_name, checked_at DESC"'

dlq: ## Summarise dead letters by stage and reason
	@$(COMPOSE) exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT stage, error_reason, COUNT(*), MAX(received_at) AS last_seen FROM dead_letter_events GROUP BY 1, 2 ORDER BY 3 DESC"'

backfill: ## Re-run NLP for posts missing from the processed table (SINCE=YYYY-MM-DD optional)
	@# Job 1 is stopped first: Delta on S3 cannot coordinate two concurrent writers.
	$(COMPOSE) stop spark-job1
	$(COMPOSE) run --rm --no-deps spark-job1 /opt/spark/bin/spark-submit --master local[2] /app/backfill.py $(if $(SINCE),--since $(SINCE),) ; \
	  status=$$? ; $(COMPOSE) start spark-job1 ; exit $$status

test: ## Run unit tests (no Docker needed)
	python -m pytest tests/unit -q

test-spark: ## Run Spark local-mode tests (needs Java 17 and pyspark)
	python -m pytest tests/spark -q

lint: ## Lint with ruff
	ruff check .

check: lint test ## Lint + unit tests
	$(COMPOSE) config --quiet && echo "docker-compose.yml OK"

sample: ## Regenerate data/sample_posts.jsonl (synthetic, for PRODUCER_MODE=replay)
	cd producer && python -c "import json, sources; s = sources.SyntheticSource(['worldnews', 'todayilearned'], posts_per_minute=20, seed=7); [print(json.dumps(p)) for p in s.poll(600, now=1.7e9)]" > ../data/sample_posts.jsonl

fetch: ## Download archived posts from Arctic Shift (SINCE=YYYY-MM-DD UNTIL=YYYY-MM-DD)
	@test -n "$(SINCE)" -a -n "$(UNTIL)" || (echo 'usage: make fetch SINCE=2026-10-14 UNTIL=2026-10-16' && exit 1)
	$(COMPOSE) build producer
	$(COMPOSE) run --rm --no-deps -v "$(CURDIR)/data:/data" producer python fetch_archive.py \
	  --since $(SINCE) --until $(UNTIL) --out-dir /data

real: ## fetch + convert + restart the producer in timed replay (SINCE= UNTIL= SPEEDUP=60)
	@test -n "$(SINCE)" -a -n "$(UNTIL)" || (echo 'usage: make real SINCE=2026-10-14 UNTIL=2026-10-16' && exit 1)
	$(MAKE) fetch SINCE=$(SINCE) UNTIL=$(UNTIL)
	$(MAKE) convert IN="$(foreach s,worldnews todayilearned,data/$(s)_submissions_$(SINCE)_$(UNTIL).jsonl)"
	PRODUCER_MODE=replay REPLAY_FILE=real_posts.jsonl REPLAY_SPEEDUP=$(or $(SPEEDUP),60) POLL_INTERVAL_SECONDS=10 \
	  $(COMPOSE) up -d --no-deps producer
	@echo "producer now replaying real posts - set the same values in .env to keep them across restarts"

convert: ## Convert archived Reddit dumps in data/ to a replay file (IN="data/a.zst data/b.zst" SINCE= UNTIL=)
	@test -n "$(IN)" || (echo 'usage: make convert IN="data/worldnews_submissions.zst data/todayilearned_submissions.zst" SINCE=2024-06-01 UNTIL=2024-06-03' && exit 1)
	$(COMPOSE) build producer
	$(COMPOSE) run --rm --no-deps -v "$(CURDIR)/data:/data" producer python convert_dump.py \
	  $(foreach f,$(IN),/data/$(notdir $(f))) --out /data/real_posts.jsonl \
	  $(if $(SINCE),--since $(SINCE),) $(if $(UNTIL),--until $(UNTIL),) $(EXTRA)

reset: ## Stop the stack and DELETE all data volumes
	@read -p "This deletes Kafka, lake, Postgres and Grafana data. Continue? [y/N] " ans && [ "$$ans" = y ]
	$(COMPOSE) --profile tools down -v
