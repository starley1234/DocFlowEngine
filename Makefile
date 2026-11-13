.PHONY: install lint format typecheck test migrate api worker up down

install:
	python -m pip install -r requirements-dev.txt -r ocr_service/requirements.txt

lint:
	ruff check src config main.py run_worker.py ocr_service migrations tests
	ruff format --check src config main.py run_worker.py ocr_service migrations tests

format:
	ruff format src config main.py run_worker.py ocr_service migrations tests
	ruff check --fix src config main.py run_worker.py ocr_service migrations tests

typecheck:
	mypy

test:
	pytest --cov=src --cov-report=term-missing

migrate:
	alembic upgrade head

api:
	uvicorn main:app --host 0.0.0.0 --port 8000

worker:
	python run_worker.py

up:
	docker compose up --build -d

down:
	docker compose down
