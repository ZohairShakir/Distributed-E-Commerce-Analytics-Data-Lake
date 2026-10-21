.PHONY: setup doctor generate-small test test-unit test-integration lint format run-local run-bronze run-silver run-pipeline analytics benchmark clean down shell

COMPOSE := docker compose
SERVICE := spark
SMALL_ROOT := data/raw/synthetic/small
SMOKE_ROOT := data/phase1/smoke
LAKE_ROOT := data/lake

setup:
	$(COMPOSE) build $(SERVICE)

doctor:
	$(COMPOSE) run --rm $(SERVICE) python scripts/doctor.py

generate-small:
	$(COMPOSE) run --rm $(SERVICE) python -m aws_spark_data_platform.generator.cli \
		--size small --output $(SMALL_ROOT) --force

test:
	$(COMPOSE) run --rm $(SERVICE) pytest

test-unit:
	$(COMPOSE) run --rm $(SERVICE) pytest tests/unit

test-integration:
	$(COMPOSE) run --rm $(SERVICE) pytest -m integration

lint:
	$(COMPOSE) run --rm $(SERVICE) ruff check src tests scripts

format:
	$(COMPOSE) run --rm $(SERVICE) ruff format src tests scripts

run-local:
	$(COMPOSE) run --rm $(SERVICE) python -m aws_spark_data_platform.orchestration.smoke_job \
		--input-root $(SMALL_ROOT) --output-root $(SMOKE_ROOT)

run-bronze:
	$(COMPOSE) run --rm $(SERVICE) python -m aws_spark_data_platform.orchestration.bronze_job \
		--source-root $(SMALL_ROOT) --lake-root $(LAKE_ROOT) --batch-id batch-0001

run-silver:
	$(COMPOSE) run --rm $(SERVICE) python -m aws_spark_data_platform.orchestration.silver_job \
		--lake-root $(LAKE_ROOT) --batch-id batch-0001

run-pipeline:
	$(COMPOSE) run --rm $(SERVICE) python -m aws_spark_data_platform.orchestration.pipeline_job \
		--source-root $(SMALL_ROOT) --lake-root $(LAKE_ROOT) --batch-id batch-0001

analytics:
	$(COMPOSE) run --rm $(SERVICE) python -m aws_spark_data_platform.orchestration.gold_job \
		--lake-root $(LAKE_ROOT) --batch-id batch-0001

benchmark:
	$(COMPOSE) run --rm $(SERVICE) python -m aws_spark_data_platform.orchestration.benchmark_job \
		--lake-root $(LAKE_ROOT) --batch-id batch-0001 --repetitions 3

clean:
	$(COMPOSE) run --rm $(SERVICE) python scripts/clean.py

down:
	$(COMPOSE) down --remove-orphans

shell:
	$(COMPOSE) run --rm $(SERVICE) bash
