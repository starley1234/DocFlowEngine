FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends poppler-utils \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 app

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY --chown=app:app src ./src
COPY --chown=app:app config ./config
COPY --chown=app:app migrations ./migrations
COPY --chown=app:app alembic.ini main.py run_worker.py ./
RUN mkdir -p storage/raw storage/artifacts storage/inbox storage/tmp \
    && chown -R app:app storage

USER app
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
