.PHONY: install test lint up down logs demo

install:
	python -m pip install -e ".[dev]"

test:
	python -m pytest -q

lint:
	python -m ruff check .

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

demo:
	python examples/receiver.py
