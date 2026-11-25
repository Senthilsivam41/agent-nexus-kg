.PHONY: setup demo dev showcase-build showcase check docker-build docker-up docker-down

setup:
	uv sync --locked

demo:
	MOCK_MODE=true uv run python -m scale_kg.demo

dev:
	uv run python -m scale_kg.api.app --host 127.0.0.1 --port 8000

showcase-build:
	uv run python -m scale_kg.showcase.build --presets micro,local --out dist/showcase

showcase: showcase-build
	SCALE_KG_SHOWCASE_SNAPSHOT=dist/showcase/snapshot.json uv run python -m scale_kg.api.app --host 127.0.0.1 --port 8000

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
