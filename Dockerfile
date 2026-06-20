FROM python:3.12.8-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN groupadd --system arbitrage && useradd --system --gid arbitrage --create-home arbitrage
WORKDIR /app

COPY pyproject.toml README.md alembic.ini ./
COPY migrations ./migrations
COPY src ./src
RUN python -m pip install --upgrade pip && python -m pip install .

USER arbitrage
EXPOSE 9108
HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9108/health/live', timeout=2)"

ENTRYPOINT ["arbitrage-engine"]
CMD ["--config", "/run/config/config.json"]
