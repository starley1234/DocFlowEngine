FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       tesseract-ocr tesseract-ocr-rus tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 app

WORKDIR /app
COPY ocr_service/requirements.txt ./ocr_service/requirements.txt
RUN pip install -r ocr_service/requirements.txt
COPY --chown=app:app ocr_service ./ocr_service

USER app
EXPOSE 8000
CMD ["uvicorn", "ocr_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
