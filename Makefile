SHELL := bash
COMPOSE := docker compose
EVENTS ?= 100000
HADOLINT := hadolint/hadolint:v2.14.0
TRIVY := aquasec/trivy:0.76.2
GITLEAKS := zricethezav/gitleaks:v8.30.1

.PHONY: up down seed test test-unit test-host test-dags test-spark test-e2e lint audit demo demo-small drills

up:
	@test -f .env || cp .env.example .env
	@# one shared Fernet key so every Airflow container can decrypt Variables
	@grep -q '^AIRFLOW_FERNET_KEY=' .env || echo 'AIRFLOW_FERNET_KEY=' >> .env
	@grep -q '^AIRFLOW_FERNET_KEY=.' .env || python3 infra/fernet_key.py .env
	$(COMPOSE) up -d --build --wait --wait-timeout 300

down:
	$(COMPOSE) down -v

seed:
	$(COMPOSE) exec -T consumer python -m ingest.seed

# fast tests that need no services
test-unit:
	uv run pytest -q

# everything against the running stack (run make demo or demo-small first)
test: test-host test-dags test-spark test-e2e

test-host:
	# the ingest kill test runs its own consumer, so the compose one steps aside
	$(COMPOSE) stop consumer
	uv run pytest -q -m "not e2e" --cov --cov-report=term --cov-report=xml || ($(COMPOSE) start consumer; exit 1)
	$(COMPOSE) start consumer
	uv run coverage report --fail-under=80 > /dev/null
	uv run coverage report --include='ingest/*' --fail-under=85
	uv run coverage report --include='quality/*' --fail-under=85
	uv run coverage report --include='finops/*' --fail-under=90
	uv run coverage report --include='mcp/*' --fail-under=85

test-dags:
	$(COMPOSE) run --rm --no-deps -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db \
		-e AIRFLOW__CORE__EXECUTOR=SequentialExecutor airflow-scheduler bash -c "airflow db migrate > /dev/null && python -m pytest tests/dags -q --cov=dags --cov-fail-under=75"

test-spark:
	$(COMPOSE) run --rm --no-deps -v ./tests:/app/tests spark \
		python3 -m pytest tests/spark -q -p no:cacheprovider --cov=spark --cov-fail-under=70

test-e2e:
	uv run pytest -q -m e2e tests/e2e

lint:
	uv run ruff check .
	uv run mypy ingest quality finops governance mcp/pipeline_mcp
	uv run bandit -q -ll -r ingest quality finops governance mcp dags spark

audit:
	uv run bandit -q -ll -r ingest quality finops governance mcp dags spark
	uv run pip-audit --strict --progress-spinner off
	# run as the checkout owner, otherwise git refuses the repo as dubious ownership
	docker run --rm --user "$$(id -u):$$(id -g)" -v "$(CURDIR):/repo" $(GITLEAKS) git /repo --no-banner
	docker run --rm -v "$(CURDIR):/repo:ro" $(TRIVY) fs --scanners vuln,secret,misconfig --severity HIGH,MEDIUM,CRITICAL \
		--skip-dirs /repo/.venv --skip-dirs /repo/.git --skip-files /repo/.coverage --exit-code 1 /repo
	for f in ingest/Dockerfile mcp/Dockerfile dags/Dockerfile spark/Dockerfile infra/otel.Dockerfile; do \
		docker run --rm -i $(HADOLINT) < $$f || exit 1; done

demo: up seed
	bash infra/demo.sh $(EVENTS)

demo-small:
	$(MAKE) demo EVENTS=10000

drills:
	bash infra/drills.sh
