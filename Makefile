.PHONY: setup demo dev check docker-build docker-up docker-down

setup:
	uv sync --locked

demo:
	MOCK_MODE=true uv run python -m scale_kg.demo

dev:
	uv run python -m scale_kg.api.app --host 127.0.0.1 --port 8000

docker-build:
	docker compose --profile neo4j --profile hugegraph --profile puppygraph build

docker-up:
	docker compose --profile neo4j --profile hugegraph --profile puppygraph up -d --build

docker-down:
	docker compose --profile neo4j --profile hugegraph --profile puppygraph down

check:
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy src
	uv run pytest
	uv build
