from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic_core import ValidationError

from app.db import check_db, close_db_pool, init_db_pool, list_user_tables
from app.llm import build_llm
from app.schema_introspection import format_schema_prompt, get_schema_snapshot_cached
from app.text2sql_agent import text_to_sql

@asynccontextmanager
async def lifespan(_: FastAPI):
    # Initialize DB pool at startup (fails fast if DATABASE_URL is missing/invalid)
    init_db_pool()
    try:
        yield
    finally:
        close_db_pool()


app = FastAPI(title="text2sql-service", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"ok": True}


class ChatRequest(BaseModel):
    message: str


@app.post("/chat")
def chat(req: ChatRequest) -> dict:
    try:
        llm = build_llm()
    except ValidationError as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Missing required env vars",
                "required": [
                    "AZURE_OPENAI_ENDPOINT",
                    "AZURE_OPENAI_API_KEY",
                    "AZURE_OPENAI_DEPLOYMENT",
                    "AZURE_OPENAI_API_VERSION",
                    "DATABASE_URL",
                ],
                "validation_errors": e.errors(),
            },
        ) from e
    resp = llm.complete(req.message)
    return {"text": str(resp)}


class Text2SQLRequest(BaseModel):
    question: str
    limit: int = 100


@app.get("/db/health")
def db_health() -> dict:
    result = check_db()
    if not result.get("ok"):
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get("/db/tables")
def db_tables() -> dict:
    result = list_user_tables()
    if not result.get("ok"):
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get("/db/schema")
def db_schema() -> dict:
    ok, payload = get_schema_snapshot_cached()
    if not ok:
        raise HTTPException(status_code=503, detail=payload)
    snapshot = payload["snapshot"]
    return {
        "ok": True,
        "cached": payload["cached"],
        "generated_at_epoch_s": snapshot.generated_at_epoch_s,
        "excluded_tables": snapshot.excluded_tables,
        "tables": [
            {
                "schema": t.schema,
                "name": t.name,
                "description": t.description,
                "columns": [
                    {"name": c.name, "data_type": c.data_type, "is_nullable": c.is_nullable}
                    for c in t.columns
                ],
            }
            for t in snapshot.tables
        ],
        "foreign_keys": [
            {
                "constraint_name": fk.constraint_name,
                "src": {"schema": fk.src_schema, "table": fk.src_table, "columns": fk.src_columns},
                "dst": {"schema": fk.dst_schema, "table": fk.dst_table, "columns": fk.dst_columns},
            }
            for fk in snapshot.foreign_keys
        ],
        "prompt": format_schema_prompt(snapshot),
    }


@app.post("/text2sql")
def text2sql(req: Text2SQLRequest) -> dict:
    ok, payload = get_schema_snapshot_cached()
    if not ok:
        raise HTTPException(status_code=503, detail=payload)
    snapshot = payload["snapshot"]

    ok2, out = text_to_sql(req.question, schema_snapshot=snapshot, limit=req.limit)
    if not ok2:
        raise HTTPException(status_code=400, detail=out)
    return {"ok": True, **out}
