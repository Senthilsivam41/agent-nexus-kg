FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PUPPYGRAPH_WAREHOUSE_DIR=/data/warehouse

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN pip install --no-cache-dir --no-compile . && mkdir -p /data/warehouse

EXPOSE 8081

HEALTHCHECK --interval=5s --timeout=2s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8081/health', timeout=1)"

ENTRYPOINT ["python", "-m", "scale_kg.adapters.container_server", "--engine", "puppygraph", "--host", "0.0.0.0", "--port", "8081"]
