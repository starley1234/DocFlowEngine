# DocFlow Engine

Асинхронный модульный микромонолит для обработки документов делопроизводства:
PDF/сканы → PNG → OCR → извлечение реквизитов LLM → эмбеддинги и семантический поиск.
Очередь задач живёт в PostgreSQL (`FOR UPDATE SKIP LOCKED`), отдельный брокер не нужен.

## Возможности

- FastAPI + Pydantic v2, асинхронный SQLAlchemy 2 и PostgreSQL 16 + pgvector.
- Потоковая загрузка, проверка сигнатур PDF/PNG/JPEG, лимиты размера и SHA-256-дедупликация.
- Очередь с lease, heartbeat, fencing token, восстановлением после падения воркера и retry backoff.
- Атомарная фиксация результата, векторов и статуса `COMPLETED`.
- JSON-конфигурации пайплайнов, изолированные узлы, строгая Pydantic-валидация ответа LLM.
- Источники: HTTP upload, локальная папка, узкий OData v4-адаптер.
- Встроенный Tesseract HTTP-адаптер для Docker Compose; LLM/embeddings — любой OpenAI-совместимый сервер.
- Миграции Alembic, тесты на настоящем PostgreSQL/pgvector, Ruff и mypy strict.

## Быстрый старт: Docker Compose

Нужны Docker Compose и внешний LLM/embedding-сервис. Например, для Ollama на хосте:

```bash
ollama pull qwen2.5:14b
ollama pull bge-m3
```

На Linux Ollama должна принимать подключения с Docker-моста (например, `OLLAMA_HOST=0.0.0.0:11434`);
не открывайте порт в интернет без фаервола/аутентификации. Для vLLM/другого сервера укажите его URL и имена моделей.

```bash
cp .env.example .env
# Обязательно замените POSTGRES_PASSWORD и DOCFLOW_API_KEY.
docker compose up --build -d

curl http://localhost:8000/health/ready
```

Swagger UI: <http://localhost:8000/docs>. OCR внутри Compose доступен как `http://ocr:8000/predict`,
с хоста — `http://localhost:8001/predict`. Сервис `migrate` один раз применяет миграции и завершается.

```bash
docker compose logs -f worker
docker compose down          # данные сохраняются в volumes
```

## Локальная разработка

Системные зависимости: Python 3.11+, Poppler (`pdftoppm`/`pdfinfo`), Tesseract с языками `rus` и `eng`.
Для Ubuntu/Debian: `sudo apt-get install poppler-utils tesseract-ocr tesseract-ocr-rus tesseract-ocr-eng`.

```bash
python -m venv .venv
source .venv/bin/activate
make install
cp .env.example .env
# Для локального Python замените host.docker.internal на localhost в URL LLM/embeddings.

docker compose up -d db
alembic upgrade head

# В отдельных терминалах:
uvicorn ocr_service.main:app --host 0.0.0.0 --port 8001
python run_worker.py
uvicorn main:app --host 0.0.0.0 --port 8000
```

## API

Если задан `DOCFLOW_API_KEY`, все маршруты `/api/v1/*` требуют `Authorization: Bearer <key>`.
Health-маршруты открыты. Новый файл возвращает `202`, дубль по SHA-256 — `200` и существующие ID.

```bash
KEY="your-api-key"

curl -X POST http://localhost:8000/api/v1/documents/upload \
  -H "Authorization: Bearer $KEY" \
  -F "file=@letter.pdf" \
  -F "pipeline_name=incoming_letter"

curl -H "Authorization: Bearer $KEY" http://localhost:8000/api/v1/tasks/<task_id>
curl -H "Authorization: Bearer $KEY" \
  "http://localhost:8000/api/v1/documents/<document_id>?include_text=true"
curl -H "Authorization: Bearer $KEY" \
  http://localhost:8000/api/v1/documents/<document_id>/download -o original.pdf

curl -X POST http://localhost:8000/api/v1/scan-folder -H "Authorization: Bearer $KEY"
curl -X POST http://localhost:8000/api/v1/tasks/<task_id>/retry -H "Authorization: Bearer $KEY"
curl -X POST http://localhost:8000/api/v1/search \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d '{"query":"претензия по оплате","limit":10}'
```

`scan-folder` сканирует только настроенную папку `storage/inbox`, путь нельзя задать через API.
Повторный запуск доступен только для терминального статуса `FAILED` и сбрасывает счётчик попыток.

## Надёжность очереди

1. Документ и задача создаются одной транзакцией; `UNIQUE(file_hash)` закрывает гонку дублей.
2. Воркер захватывает строку через `FOR UPDATE SKIP LOCKED`, ставит lease и увеличивает fencing token.
3. Heartbeat продлевает lease. Устаревший воркер не может записать результат после перехвата задачи.
4. Временные ошибки провайдеров возвращают задачу в `QUEUED` с экспоненциальной задержкой.
   Невалидные данные/конфигурация/ответ LLM сразу дают `FAILED`.
5. `max_retries` — лимит неудачных попыток до `FAILED`, включая попытки, потерянные из-за истёкшего lease.
6. OCR, извлечённые данные и векторы сохраняются атомарно на финальном шаге. Артефакты попыток изолированы.

## Конфигурация

Настройки читаются из окружения с префиксом `DOCFLOW_` и `.env`. Основные параметры:

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DOCFLOW_DATABASE_URL` | `postgresql+asyncpg://…@localhost/docflow` | PostgreSQL, только драйвер asyncpg |
| `DOCFLOW_API_KEY` | пусто | Bearer-ключ; в production обязателен, минимум 16 символов |
| `DOCFLOW_OCR_URL` | `http://localhost:8001/predict` | Endpoint OCR |
| `DOCFLOW_LLM_BASE_URL` | `http://localhost:11434/v1` | OpenAI-совместимый base URL |
| `DOCFLOW_LLM_MODEL` | `qwen2.5:14b` | Модель извлечения |
| `DOCFLOW_EMBEDDING_MODEL` | `bge-m3` | Модель эмбеддингов, размерность 1024 |
| `DOCFLOW_MAX_UPLOAD_BYTES` | `20971520` | Лимит входа |
| `DOCFLOW_MAX_PDF_PAGES` | `100` | Лимит страниц PDF |
| `DOCFLOW_WORKER_LEASE_SECONDS` | `300` | Время жизни захвата |
| `DOCFLOW_WORKER_HEARTBEAT_SECONDS` | `30` | Интервал продления, меньше половины lease |

Пайплайн лежит в `config/pipelines/incoming_letter.json`. Адрес OCR можно переопределить параметром
узла `service_url`, модель LLM — параметром `model`. Для другого LLM-сервера должен поддерживаться
JSON mode (`response_format: {"type":"json_object"}`); его можно отключить параметром `json_mode: false`.
Размерность вектора зафиксирована миграцией: смена модели на другую размерность требует новой миграции.

### Контракты провайдеров

OCR: `POST multipart/form-data`, поля `file` (PNG) и `language` (`rus+eng`), ответ:

```json
{"text": "распознанный текст", "confidence": 0.97}
```

LLM: `POST {base_url}/chat/completions`; embeddings: `POST {base_url}/embeddings`.
Секреты передаются только заголовком Bearer и не попадают в логи/карточку задачи.

OData-адаптер ожидает feed с `value[]`, полями `file_name`, `content_url` и пагинацией
`@odata.nextLink`. Ссылки принимаются только с origin настроенного feed. Это явный контракт для
интеграции с 1С/шлюзом; произвольные сущности 1С требуют отдельного маппинга.

## Тесты и качество

```bash
make lint
make typecheck
make test
```

По умолчанию тесты поднимают временный PostgreSQL 16 + pgvector через `pgserver` из dev-зависимостей.
Можно использовать внешний сервер: `TEST_DATABASE_URL=postgresql+asyncpg://…/docflow_test`.
Имя базы обязано заканчиваться на `_test`: fixture выполняет `TRUNCATE`, не используйте рабочую базу.
Тесты с реальными Poppler/Tesseract пропускаются локально, если бинарники не установлены; CI их ставит.

## Структура

```text
config/          настройки и JSON-пайплайны
migrations/      Alembic
src/api/         FastAPI
src/models/      таблицы и Pydantic-контракты
src/queue/       producer, SKIP LOCKED, worker
src/pipeline/    конфигурация, контекст, registry, runner
src/nodes/       converter, OCR, LLM, vectors
src/sources/     folder, api upload, OData
src/providers/   HTTP/LLM/embedding-адаптеры
ocr_service/     Tesseract HTTP-сервис
storage/         raw, artifacts, inbox, tmp (не коммитится)
```

## Эксплуатация и безопасность

- Не публикуйте API без ключа/reverse proxy; `DOCFLOW_ENVIRONMENT=production` требует сильный ключ.
- PostgreSQL в Compose привязан к `127.0.0.1`; файлы и БД хранятся в Docker volumes.
- Текст документа передаётся LLM как данные с инструкцией игнорировать команды внутри документа,
  но это не заменяет изоляцию недоверенного контента и контроль исходящего трафика.
- Артефакты попыток сохраняются для аудита; настройте ротацию/очистку по вашей политике хранения.
- LLM не входит в Compose: выбор GPU/модели зависит от инфраструктуры. Без доступного LLM/embedding-сервиса
  задачи будут закономерно падать после retry.
