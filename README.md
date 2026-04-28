# text2sql-service

FastAPI service scaffolded with LlamaIndex + Azure OpenAI + env-based config.

## Quickstart

Create and activate a virtualenv, then install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` from the example and fill in values:

```bash
cp .env.example .env
```

Run the API:

```bash
uvicorn app.main:app --reload --port 8000
```

## Endpoints

- `GET /health`: basic health check
- `GET /db/health`: checks DB connectivity (`SELECT 1`)
- `GET /db/tables`: lists non-system tables (schema + table name)
- `POST /chat`: sends a message to Azure OpenAI via LlamaIndex

Example:

```bash
curl -sS http://localhost:8000/health
curl -sS http://localhost:8000/db/health
curl -sS http://localhost:8000/db/tables

curl -sS http://localhost:8000/chat \
  -H 'content-type: application/json' \
  -d '{"message":"Say hello"}'
```
