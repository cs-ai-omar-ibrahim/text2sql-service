# text2sql-service

FastAPI service for sending natural language SQL queries to the [TMS](https://github.com/clearsky-ai/task-management) built with LlamaIndex + Azure OpenAI.

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
- `GET /db/schema`: schema snapshot + prompt material (cached with TTL)
- `POST /text2sql`: generate SQL (Postgres, SELECT-only) and execute it
- `POST /chat`: sends a message to Azure OpenAI

Example:

```bash
curl -sS http://localhost:8000/health
curl -sS http://localhost:8000/db/health
curl -sS http://localhost:8000/db/tables
curl -sS http://localhost:8000/db/schema

curl -sS http://localhost:8000/chat \
  -H 'content-type: application/json' \
  -d '{"message":"Say hello"}'

curl -sS http://localhost:8000/text2sql \
  -H 'content-type: application/json' \
  -d '{"question":"Show all task types that start with the word Send","limit":10}'
```
