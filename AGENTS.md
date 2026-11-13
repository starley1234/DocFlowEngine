# Техническое задание: DocFlow Engine (Кодовое имя: «Обработка Документооборота»)

Модульный микромонолит на Python для асинхронного извлечения атрибутов, классификации и семантического анализа документов делопроизводства (сканы, PDF) с очередью в PostgreSQL.

---

## 1. Архитектурный стек

* **Ядро:** Python 3.11+, FastAPI (REST API), Pydantic v2 (валидация).
* **СУБД и Очередь:** PostgreSQL 16 + расширение `pgvector`. Очередь задач реализована внутри PG на паттерне `FOR UPDATE SKIP LOCKED`.
* **Хранилище артефактов:** Локальная FS (`./storage/docs/` и `./storage/artifacts/`).
* **AI/OCR провайдеры:** 
  * OCR: Внешний сервис (Tesseract / EasyOCR / PaddleOCR-контейнер).
  * LLM/VLM: OpenAI-совместимый API (vLLM, llama.cpp, Ollama).

---

## 2. Файловая структура

```text
docflow-engine/
├── config/
│   ├── settings.py            # Pydantic Settings (.env: PG DSN, LLM URL, OCR URL)
│   └── pipelines/             # JSON-конфигурации пайплайнов
│       └── incoming_letter.json
├── storage/                   # Локальное хранилище файлов (.gitignore кроме .keep)
│   ├── raw/                   # Исходные файлы по doc_id
│   └── artifacts/             # Нарезка PNG, OCR-json
├── src/
│   ├── database.py            # Async SQLAlchemy engine + sessionmaker
│   ├── models/
│   │   ├── db.py              # Таблицы: Document, QueueTask, DocumentVector
│   │   └── schemas.py         # Pydantic-контракты (IncomingLetterSchema и др.)
│   ├── queue/
│   │   ├── producer.py        # Постановка задач в PG
│   │   └── worker.py          # Воркер: цикл SELECT ... FOR UPDATE SKIP LOCKED
│   ├── sources/               # Источники входных данных
│   │   ├── base.py            # BaseSource (ABC)
│   │   ├── folder.py          # Сканер локальной папки
│   │   ├── odata.py           # Коннектор к 1С (заглушка/реализация)
│   │   └── api_upload.py      # Обработчик прямых HTTP-загрузок
│   ├── nodes/                 # Атомарные шаги пайплайна
│   │   ├── base.py            # BaseNode (ABC)
│   │   ├── pdf_converter.py   # Конвертация PDF -> PNG (pdf2image)
│   │   ├── ocr_node.py        # Запрос к OCR-сервису
│   │   ├── llm_extractor.py   # Вызов LLM с Pydantic/JSON-mode
│   │   └── vector_node.py     # Получение эмбеддинга и запись в pgvector
│   ├── pipeline/
│   │   ├── registry.py        # Реестр доступных Nodes
│   │   └── runner.py          # Исполнитель шагов по pipeline.json
│   └── api/
│       ├── main.py            # FastAPI app
│       └── routes.py          # /upload, /tasks/{id}, /documents/{id}
├── main.py                    # Точка входа API
├── run_worker.py              # Точка входа background-воркера
└── requirements.txt
```

---

## 3. Схема базы данных (SQL DDL)

```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;

-- Статусы задач
CREATE TYPE task_status AS ENUM (
    'QUEUED', 'CONVERTING', 'OCR', 'EXTRACTING', 'INDEXING', 'COMPLETED', 'FAILED'
);

-- Таблица документов (артефакты и метаданные)
CREATE TABLE documents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64) UNIQUE NOT NULL, -- SHA-256 для дедупликации
    file_path TEXT NOT NULL,
    source_type VARCHAR(50) NOT NULL,      -- 'folder', 'api', 'odata'
    pipeline_name VARCHAR(100) NOT NULL,
    extracted_data JSONB DEFAULT '{}',     -- Результат Pydantic-валидации
    raw_ocr_text TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Векторные чанки (RAG)
CREATE TABLE document_embeddings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    chunk_index INT NOT NULL,
    content TEXT NOT NULL,
    embedding vector(1024)                 -- Размерность под модель (напр. BGE-M3)
);

-- Очередь задач (SKIP LOCKED)
CREATE TABLE task_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    pipeline_name VARCHAR(100) NOT NULL,
    status task_status DEFAULT 'QUEUED',
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    error_message TEXT,
    locked_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_task_queue_fetch ON task_queue (status, created_at) 
WHERE status = 'QUEUED';
```

---

## 4. Пайплайн №1: «Входящее письмо»

### Pydantic-схема (`src/models/schemas.py`)
```python
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date

class IncomingLetterSchema(BaseModel):
    sender_org: Optional[str] = Field(None, description="Организация-отправитель")
    recipient_org: Optional[str] = Field(None, description="Организация-получатель")
    sender_person: Optional[str] = Field(None, description="От кого (ФИО, должность отправителя)")
    recipient_person: Optional[str] = Field(None, description="Кому адресовано (ФИО, должность)")
    incoming_number: Optional[str] = Field(None, description="Входящий регистрационный номер")
    outgoing_number: Optional[str] = Field(None, description="Исходящий номер отправителя")
    deadline: Optional[date] = Field(None, description="Срок исполнения документа, если указан")
    tags: List[str] = Field(default_factory=list, description="Теги документа (напр.: 'претензия', 'оплата', 'гарантия')")
    summary: str = Field(..., description="Краткое содержание письма в 2-3 предложениях")
```

### JSON-конфигурация (`config/pipelines/incoming_letter.json`)
```json
{
  "pipeline_name": "incoming_letter",
  "version": "1.0",
  "schema_model": "IncomingLetterSchema",
  "nodes": [
    {
      "node_name": "pdf_converter",
      "params": { "dpi": 200, "first_page_only": false }
    },
    {
      "node_name": "ocr_node",
      "params": { "service_url": "http://ocr:8000/predict" }
    },
    {
      "node_name": "llm_extractor",
      "params": {
        "model": "qwen2.5-14b-instruct",
        "temperature": 0.0,
        "max_pages_to_llm": 2,
        "system_prompt": "Ты — эксперт канцелярии. Извлеки данные строго в JSON по заданной схеме. Не выдумывай отсутствующие реквизиты."
      }
    },
    {
      "node_name": "vector_node",
      "params": { "chunk_size": 1000, "overlap": 100 }
    }
  ]
}
```

---

## 5. Механика очереди задач (`SKIP LOCKED`)

Воркер (`src/queue/worker.py`) циклично выполняет выборку:

```sql
UPDATE task_queue
SET status = 'CONVERTING', 
    locked_at = NOW(),
    updated_at = NOW()
WHERE id = (
    SELECT id
    FROM task_queue
    WHERE status = 'QUEUED' AND retry_count < max_retries
    ORDER BY created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, document_id, pipeline_name;
```

**Логика выполнения:**
1. Если запись получена: воркер находит документ, загружает `config/pipelines/{pipeline_name}.json`.
2. Последовательно передает контекст (`doc_path`, `ocr_text`, `images`, `extracted_data`) сквозь узлы пайплайна.
3. По мере шагов статус в `task_queue` меняется: `CONVERTING` → `OCR` → `EXTRACTING` → `INDEXING`.
4. Финиш: `extracted_data` пишется в `documents`, статус задачи переходит в `COMPLETED`.
5. Ошибка: перехват исключения, запись `error_message`, `retry_count += 1`, статус переводится в `FAILED`.

---

## 6. Базовый интерфейс узла пайплайна (`src/nodes/base.py`)

Каждый шаг изолирован и реализует контракт:

```python
from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseNode(ABC):
    def __init__(self, params: Dict[str, Any]):
        self.params = params

    @abstractmethod
    async def process(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Принимает context (содержит doc_id, file_path, ocr_text и т.д.),
        производит обработку, обновляет и возвращает context.
        """
        pass
```

---

## 7. Входные интерфейсы (FastAPI Endpoints)

* `POST /api/v1/documents/upload`
  Загружает файл, считает `sha256`. Если дубль — возвращает существующий `document_id`. Если новый — сохраняет в `storage/raw/`, создает запись в `documents` и ставит задачу в `task_queue`. Возвращает `{"task_id": "...", "document_id": "..."}` со статусом 202 Accepted.

* `GET /api/v1/tasks/{task_id}`
  Опрос статуса (`QUEUED`, `OCR`, `EXTRACTING`, `COMPLETED`, `FAILED`).

* `GET /api/v1/documents/{document_id}`
  Возврат готовой карточки: `extracted_data` (JSON схемы `IncomingLetterSchema`), путь к файлу, даты обработки.

* `POST /api/v1/scan-folder`
  Служебный эндпоинт запускает сканер `FolderSource`: прогоняет локальную папку с 50 PDF, хеширует, ставит недостающие файлы в очередь.