from __future__ import annotations

from typing import Any, Dict, List

from app.db import close_db_pool, init_db_pool
from app.schema_introspection import get_db_schema_payload


def _to_sql_data_type(data_type: str) -> str:
    """
    Best-effort conversion from introspected Postgres type names to SQL-ish DDL style.
    """
    t = (data_type or "").strip().lower()
    if t == "character varying":
        return "VARCHAR"
    if t == "timestamp without time zone":
        return "TIMESTAMP"
    if t == "timestamp with time zone":
        return "TIMESTAMPTZ"
    if t == "double precision":
        return "DOUBLE PRECISION"
    if t == "integer":
        return "INTEGER"
    if t == "bigint":
        return "BIGINT"
    if t == "smallint":
        return "SMALLINT"
    if t == "boolean":
        return "BOOLEAN"
    if t == "date":
        return "DATE"
    if t == "numeric":
        return "DECIMAL"
    return data_type.upper()


def schema_payload_to_create_table_text(schema_payload: Dict[str, Any]) -> str:
    """
    Convert `get_db_schema_payload()` output to CREATE TABLE blocks + join hints.
    """
    tables: List[Dict[str, Any]] = schema_payload.get("tables", []) or []
    foreign_keys: List[Dict[str, Any]] = schema_payload.get("foreign_keys", []) or []

    # Build a quick FK lookup so we can annotate likely primary key columns.
    referenced_columns = {
        (fk.get("dst", {}).get("table"), col)
        for fk in foreign_keys
        for col in (fk.get("dst", {}).get("columns") or [])
    }

    create_blocks: List[str] = []
    for table in tables:
        table_name = table.get("name", "")
        singular_table_name = table_name[:-1] if table_name.endswith("s") else table_name
        likely_pk_names = {f"{singular_table_name}_id", "id"}
        columns = table.get("columns", []) or []

        column_lines: List[str] = []
        for col in columns:
            col_name = col.get("name", "")
            col_type = _to_sql_data_type(str(col.get("data_type", "TEXT")))
            is_nullable = bool(col.get("is_nullable", True))

            line = f"  {col_name} {col_type}"
            if not is_nullable:
                line += " NOT NULL"

            # If a column is commonly used as FK target, annotate as likely PK.
            if (table_name, col_name) in referenced_columns or col_name in likely_pk_names:
                line += " PRIMARY KEY"

            column_lines.append(line)

        create_sql = "CREATE TABLE " + table_name + " (\n" + ",\n".join(column_lines) + "\n);"
        create_blocks.append(create_sql)

    join_hints: List[str] = []
    for fk in foreign_keys:
        src = fk.get("src", {})
        dst = fk.get("dst", {})
        src_table = src.get("table")
        dst_table = dst.get("table")
        src_columns = src.get("columns") or []
        dst_columns = dst.get("columns") or []

        for src_col, dst_col in zip(src_columns, dst_columns):
            join_hints.append(f"-- {src_table}.{src_col} can be joined with {dst_table}.{dst_col}")

    sections = create_blocks[:]
    if join_hints:
        sections.append("\n".join(join_hints))
    return "\n\n".join(sections)


def get_schema_as_create_table_text() -> str:
    """
    Fetch schema from DB and convert it to DDL-like text.
    """
    init_db_pool()
    try:
        ok, payload = get_db_schema_payload()
        if not ok:
            raise RuntimeError(f"Schema error: {payload}")
        return schema_payload_to_create_table_text(payload)
    finally:
        close_db_pool()


if __name__ == "__main__":
    print(get_schema_as_create_table_text())
