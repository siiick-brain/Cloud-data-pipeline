SHELL := bash
COMPOSE := docker compose
EVENTS ?= 100000

.PHONY: up down seed test test-host test-dags test-spark lint demo demo-small

up:
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build --wait --wait-timeout 300

down:
	$(COMPOSE) down -v

seed:
	$(COMPOSE) exec -T consumer python -m ingest.seed

test: test-host test-dags test-spark

test-host:
	uv run pytest -m "not e2e" --cov --cov-report=term --cov-report=xml

test-dags:
	$(COMPOSE) exec -T airflow-scheduler python -m pytest tests/dags -q

test-spark:
	$(COMPOSE) exec -T spark python3 -m pytest tests/spark -q

lint:
	uv run ruff check .
	uv run mypy ingest quality finops governance mcp/pipeline_mcp
	uv run bandit -q -r ingest quality finops governance mcp dags spark -x tests

demo: up seed
	bash infra/demo.sh $(EVENTS)

demo-small:
	$(MAKE) demo EVENTS=10000
