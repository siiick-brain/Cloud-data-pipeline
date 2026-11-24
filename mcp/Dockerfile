# Built from the repo root: docker build -f mcp/Dockerfile .
FROM python:3.11.18-slim AS deps
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/uv
WORKDIR /build
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project --format requirements-txt -o requirements.txt \
    && pip install --no-cache-dir --require-hashes --prefix=/install -r requirements.txt

FROM python:3.11.18-slim
RUN useradd --create-home --uid 10001 app
WORKDIR /app
COPY --from=deps /install /usr/local
COPY mcp/pipeline_mcp/ pipeline_mcp/
USER 10001
ENV PYTHONUNBUFFERED=1
EXPOSE 8765
CMD ["python", "-m", "pipeline_mcp.server"]
