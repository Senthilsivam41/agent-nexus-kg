.PHONY: setup demo check

setup:
	uv sync --locked

demo:
	MOCK_MODE=true uv run python -m scale_kg.demo

check:
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy src
	uv run pytest
	uv build
