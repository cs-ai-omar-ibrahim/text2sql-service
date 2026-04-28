from fastapi import FastAPI
from pydantic import BaseModel

from app.llm import build_llm

app = FastAPI(title="text2sql-service")


@app.get("/health")
def health() -> dict:
    return {"ok": True}


class ChatRequest(BaseModel):
    message: str


@app.post("/chat")
def chat(req: ChatRequest) -> dict:
    llm = build_llm()
    resp = llm.complete(req.message)
    return {"text": str(resp)}
