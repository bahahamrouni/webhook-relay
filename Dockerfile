FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RELAY_DB_PATH=/data/relay.db

RUN useradd --create-home --uid 10001 relay && mkdir /data && chown relay /data

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

USER relay
VOLUME ["/data"]
EXPOSE 8000

CMD ["uvicorn", "--factory", "webhook_relay.api:create_app_from_env", "--host", "0.0.0.0", "--port", "8000"]
