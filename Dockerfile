# syntax=docker/dockerfile:1.7

############################
# Build stage
############################
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --upgrade pip build \
    && pip install --prefix=/install .

############################
# Runtime stage
############################
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PATH="/usr/local/bin:${PATH}"

RUN groupadd --system --gid 1000 app \
    && useradd --system --uid 1000 --gid app --shell /usr/sbin/nologin app

COPY --from=builder /install /usr/local

USER app
WORKDIR /home/app

EXPOSE 9100

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,os,sys; \
port=os.environ.get('MDC_METRICS_PORT','9100'); \
sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=3).status==200 else 1)" \
    || exit 1

ENTRYPOINT ["python", "-m", "market_data_collector"]
