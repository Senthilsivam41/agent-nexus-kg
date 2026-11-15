FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN pip install --no-cache-dir --no-compile .

EXPOSE 7474 7687

HEALTHCHECK --interval=5s --timeout=2s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:7474/health', timeout=1)"

ENTRYPOINT ["python", "-m", "scale_kg.adapters.container_server", "--engine", "neo4j", "--host", "0.0.0.0", "--port", "7474"]
