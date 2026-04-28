from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from llama_index.core.prompts import PromptTemplate
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.core.utilities.sql_wrapper import SQLDatabase
from psycopg import Connection
from psycopg.errors import OperationalError
from sqlalchemy import create_engine

from app.config import get_settings
from app.db import get_db_pool
from app.llm import build_llm
from app.schema_introspection import SchemaSnapshot, format_schema_prompt


DEFAULT_LIMIT = 100


@dataclass(frozen=True)
class Text2SQLResult:
    sql: str
    columns: List[str]
    rows: List[List[Any]]


def _normalize_sqlalchemy_url(url: str) -> str:
    # SQLAlchemy needs an explicit driver. We use psycopg3.
    if url.startswith("postgresql+psycopg://"):
        return url
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url.removeprefix("postgresql://")
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url.removeprefix("postgres://")
    return url


def _ensure_limit(sql: str, limit: int) -> str:
    # If the model didn't include a LIMIT, enforce one.
    if re.search(r"\blimit\b", sql, flags=re.IGNORECASE):
        return sql
    trimmed = sql.rstrip().rstrip(";").rstrip()
    return f"{trimmed}\nLIMIT {int(limit)};"


_DISALLOWED = re.compile(
    r"\b("
    r"insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|comment|vacuum|analyze|call|do|copy"
    r"|pg_sleep"
    r")\b",
    flags=re.IGNORECASE,
)


def _validate_readonly_sql(sql: str) -> None:
    candidate = sql.strip()

    # Single-statement only.
    parts = [p.strip() for p in candidate.split(";") if p.strip()]
    if len(parts) != 1:
        raise ValueError("Only a single SELECT statement is allowed.")

    # Must begin with SELECT or WITH.
    leading = parts[0].lstrip()
    if not re.match(r"^(select|with)\b", leading, flags=re.IGNORECASE):
        raise ValueError("Only SELECT queries are allowed.")

    if _DISALLOWED.search(leading):
        raise ValueError("DML/DDL and unsafe functions are not allowed.")


def _build_text_to_sql_prompt() -> PromptTemplate:
    # Keep variables consistent with DEFAULT_TEXT_TO_SQL_PROMPT: {dialect} {schema} {query_str}
    template = """You are a senior analytics engineer.

Rules:
- Postgres only.
- Use ONLY the tables/columns present in the provided schema.
- Prefer explicit JOINs using foreign keys when possible.
- ALWAYS include a LIMIT clause.
- NO DDL or DML (no CREATE/ALTER/DROP/INSERT/UPDATE/DELETE/TRUNCATE/etc).

Given an input question, create a syntactically correct {dialect} query to run.

You must use the following format, each taking one line:
Question: ...
SQLQuery: ...

Only use tables listed below.
{schema}

Question: {query_str}
SQLQuery: """
    return PromptTemplate(template=template)


def build_sql_query_engine(schema_snapshot: SchemaSnapshot) -> NLSQLTableQueryEngine:
    settings = get_settings()

    # Restrict to exposed tables.
    table_names = sorted({t.name for t in schema_snapshot.tables})
    if not table_names:
        raise ValueError("No tables available after allowlist/denylist filtering.")

    # SQLAlchemy engine is only used for schema/table context generation (sql_only=True prevents execution).
    sa_url = _normalize_sqlalchemy_url(settings.database_url)
    engine = create_engine(sa_url, pool_pre_ping=True)

    sql_db = SQLDatabase(
        engine,
        include_tables=table_names,
        sample_rows_in_table_info=0,
        indexes_in_table_info=False,
        view_support=True,
    )

    # Add our richer schema context (including FKs + optional descriptions).
    context_prefix = format_schema_prompt(schema_snapshot)

    return NLSQLTableQueryEngine(
        sql_database=sql_db,
        llm=build_llm(),
        text_to_sql_prompt=_build_text_to_sql_prompt(),
        tables=table_names,
        context_str_prefix=context_prefix,
        synthesize_response=False,
        sql_only=True,
        # LlamaIndex otherwise defaults to OpenAI embeddings (OPENAI_API_KEY).
        embed_model="local",
    )


def generate_sql(question: str, schema_snapshot: SchemaSnapshot, limit: int = DEFAULT_LIMIT) -> str:
    qe = build_sql_query_engine(schema_snapshot)
    resp = qe.query(question)
    sql = resp.metadata.get("sql_query") or str(resp)
    sql = str(sql).strip()
    _validate_readonly_sql(sql)
    sql = _ensure_limit(sql, limit=limit)
    _validate_readonly_sql(sql)
    return sql


def execute_sql(sql: str) -> Text2SQLResult:
    _validate_readonly_sql(sql)
    pool = get_db_pool()
    with pool.connection() as conn:  # type: Connection
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d.name for d in (cur.description or [])]
            rows = cur.fetchall() or []
    return Text2SQLResult(sql=sql, columns=cols, rows=[list(r) for r in rows])


def text_to_sql(question: str, schema_snapshot: SchemaSnapshot, limit: int = DEFAULT_LIMIT) -> Tuple[bool, Dict[str, Any]]:
    """
    Full flow: question + schema -> SQL -> execute -> structured JSON payload.
    """
    try:
        sql = generate_sql(question, schema_snapshot=schema_snapshot, limit=limit)
        result = execute_sql(sql)
        return True, {"sql": result.sql, "columns": result.columns, "rows": result.rows}
    except (OperationalError, ValueError) as e:
        return False, {"error": str(e)}
