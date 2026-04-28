from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic_core import ValidationError

from app.db import check_db, close_db_pool, init_db_pool, list_user_tables
from app.llm import build_llm

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
