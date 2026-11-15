.PHONY: setup demo check docker-build docker-up docker-down

setup:
	uv sync --locked

demo:
	MOCK_MODE=true uv run python -m scale_kg.demo

docker-build:
	docker compose build

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

check:
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy src
	uv run pytest
	uv build
