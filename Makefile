PY ?= python

.PHONY: install lint type test test-unit test-integration run bench bench-quick bench-gpu \
        docker-build compose-up compose-smoke compose-down clean

install:
	$(PY) -m pip install -e ".[dev]"

lint:
	$(PY) -m ruff check .

type:
	$(PY) -m mypy sentinel

test-unit:
	$(PY) -m pytest tests/unit -q

test-integration:
	$(PY) -m pytest tests/integration -q --timeout=180

test: lint type test-unit test-integration

run:                      ## controller + 3 local CPU workers; dashboard on :8090
	$(PY) -m sentinel controller --config config/sentinel.toml --log-format text

bench:                    ## full suite, 5 repeats per fault (~15 min)
	$(PY) -m sentinel bench --repeats 5

bench-quick:
	$(PY) -m sentinel bench --quick

bench-gpu:                ## same suite on CUDA workers (needs torch)
	$(PY) -m sentinel bench --engine torch --repeats 5 --set limits.gpu_mem_mb=6000

docker-build:
	docker compose build

compose-up:
	docker compose up -d

compose-smoke:
	bash scripts/compose_smoke.sh

compose-down:
	docker compose down -v

clean:
	rm -rf data .pytest_cache .mypy_cache .ruff_cache build *.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
