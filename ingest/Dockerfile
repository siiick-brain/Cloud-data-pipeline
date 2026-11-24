# Built from the repo root: docker build -f ingest/Dockerfile .
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
COPY docs/contracts/ docs/contracts/
COPY docs/data_dictionary.csv docs/
COPY ingest/ ingest/
COPY finops/ finops/
COPY governance/ governance/
# the demo copies cost reports out of here, so the app user must own it
RUN mkdir -p finops/out && chown 10001 finops/out
USER 10001
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "ingest.consumer"]
