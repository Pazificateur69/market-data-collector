.PHONY: install dev lint typecheck test run docker-build docker-up docker-down clean

PY ?= python3

install:
	$(PY) -m pip install -e ".[dev]"

dev: install

lint:
	ruff check src tests
	ruff format --check src tests

format:
	ruff format src tests
	ruff check --fix src tests

typecheck:
	mypy src

test:
	pytest -q

check: lint typecheck test

run:
	$(PY) -m market_data_collector

docker-build:
	docker build -t market-data-collector:latest .

docker-up:
	docker compose up --build

docker-down:
	docker compose down -v

clean:
	rm -rf build dist *.egg-info .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
