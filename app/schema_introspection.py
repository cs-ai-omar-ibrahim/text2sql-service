from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from psycopg import Connection
from psycopg.errors import OperationalError

from app.config import get_settings
from app.db import get_db_pool


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool


@dataclass(frozen=True)
class ForeignKeyInfo:
    src_schema: str
    src_table: str
    src_columns: List[str]
    dst_schema: str
    dst_table: str
    dst_columns: List[str]
    constraint_name: str


@dataclass(frozen=True)
class TableInfo:
    schema: str
    name: str
    description: Optional[str]
    columns: List[ColumnInfo]


@dataclass(frozen=True)
class SchemaSnapshot:
    generated_at_epoch_s: int
    tables: List[TableInfo]
    foreign_keys: List[ForeignKeyInfo]
    excluded_tables: List[str]


_cache_lock = threading.Lock()
_cache_value: Optional[SchemaSnapshot] = None
_cache_expires_at: float = 0.0


def _parse_csv(value: Optional[str]) -> Optional[set[str]]:
    if not value:
        return None
    items = [v.strip() for v in value.split(",")]
    items = [v for v in items if v]
    return set(items) if items else None


def _table_key(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def _is_table_exposed(schema: str, table: str, allow: Optional[set[str]], deny: Optional[set[str]]) -> bool:
    key = _table_key(schema, table)
    if deny and (key in deny or table in deny):
        return False
    if allow is None:
        return True
    return key in allow or table in allow


def _fetch_table_descriptions(conn: Connection) -> Dict[str, str]:
    # Use pg_description to pull optional comments on tables.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname AS schemaname,
                   c.relname AS tablename,
                   d.description AS description
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
            WHERE c.relkind IN ('r','p','v','m','f')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema');
            """
        )
        rows = cur.fetchall() or []
    out: Dict[str, str] = {}
    for schemaname, tablename, description in rows:
        if description:
            out[_table_key(schemaname, tablename)] = str(description)
    return out


def _fetch_columns(conn: Connection) -> Dict[str, List[ColumnInfo]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema,
                   table_name,
                   column_name,
                   data_type,
                   is_nullable,
                   ordinal_position
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name, ordinal_position;
            """
        )
        rows = cur.fetchall() or []

    by_table: Dict[str, List[ColumnInfo]] = {}
    for schema, table, col, data_type, is_nullable, _pos in rows:
        key = _table_key(schema, table)
        by_table.setdefault(key, []).append(
            ColumnInfo(name=str(col), data_type=str(data_type), is_nullable=str(is_nullable).upper() == "YES")
        )
    return by_table


def _fetch_foreign_keys(conn: Connection) -> List[ForeignKeyInfo]:
    # Uses pg_constraint to obtain multi-column FK mappings with correct ordering.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                con.conname AS constraint_name,
                ns_src.nspname AS src_schema,
                src.relname AS src_table,
                ARRAY_AGG(att_src.attname ORDER BY s.i) AS src_columns,
                ns_dst.nspname AS dst_schema,
                dst.relname AS dst_table,
                ARRAY_AGG(att_dst.attname ORDER BY s.i) AS dst_columns
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class src ON src.oid = con.conrelid
            JOIN pg_catalog.pg_namespace ns_src ON ns_src.oid = src.relnamespace
            JOIN pg_catalog.pg_class dst ON dst.oid = con.confrelid
            JOIN pg_catalog.pg_namespace ns_dst ON ns_dst.oid = dst.relnamespace
            JOIN LATERAL generate_subscripts(con.conkey, 1) AS s(i) ON TRUE
            JOIN pg_catalog.pg_attribute att_src
              ON att_src.attrelid = con.conrelid AND att_src.attnum = con.conkey[s.i]
            JOIN pg_catalog.pg_attribute att_dst
              ON att_dst.attrelid = con.confrelid AND att_dst.attnum = con.confkey[s.i]
            WHERE con.contype = 'f'
              AND ns_src.nspname NOT IN ('pg_catalog', 'information_schema')
              AND ns_dst.nspname NOT IN ('pg_catalog', 'information_schema')
            GROUP BY
                con.conname, ns_src.nspname, src.relname, ns_dst.nspname, dst.relname
            ORDER BY ns_src.nspname, src.relname, con.conname;
            """
        )
        rows = cur.fetchall() or []

    fks: List[ForeignKeyInfo] = []
    for constraint_name, src_schema, src_table, src_cols, dst_schema, dst_table, dst_cols in rows:
        fks.append(
            ForeignKeyInfo(
                constraint_name=str(constraint_name),
                src_schema=str(src_schema),
                src_table=str(src_table),
                src_columns=[str(c) for c in (src_cols or [])],
                dst_schema=str(dst_schema),
                dst_table=str(dst_table),
                dst_columns=[str(c) for c in (dst_cols or [])],
            )
        )
    return fks


def introspect_schema_snapshot() -> SchemaSnapshot:
    settings = get_settings()
    allow = _parse_csv(settings.schema_table_allowlist)
    deny = _parse_csv(settings.schema_table_denylist)

    pool = get_db_pool()
    with pool.connection() as conn:  # type: Connection
        desc_by_table = _fetch_table_descriptions(conn)
        cols_by_table = _fetch_columns(conn)
        fks = _fetch_foreign_keys(conn)

    # Build a stable list of tables from columns (covers views/materialized views too via information_schema).
    excluded: List[str] = []
    tables: List[TableInfo] = []
    for key in sorted(cols_by_table.keys()):
        schema, table = key.split(".", 1)
        if not _is_table_exposed(schema, table, allow, deny):
            excluded.append(key)
            continue
        tables.append(
            TableInfo(
                schema=schema,
                name=table,
                description=desc_by_table.get(key),
                columns=cols_by_table[key],
            )
        )

    # Filter FKs to exposed tables only.
    exposed_set = {_table_key(t.schema, t.name) for t in tables}
    fks = [
        fk
        for fk in fks
        if _table_key(fk.src_schema, fk.src_table) in exposed_set
        and _table_key(fk.dst_schema, fk.dst_table) in exposed_set
    ]

    return SchemaSnapshot(
        generated_at_epoch_s=int(time.time()),
        tables=tables,
        foreign_keys=fks,
        excluded_tables=excluded,
    )


def get_schema_snapshot_cached() -> Tuple[bool, Dict[str, Any]]:
    """
    Returns (ok, payload). Never raises OperationalError to callers.
    """
    global _cache_value, _cache_expires_at
    settings = get_settings()
    ttl = max(0, int(settings.schema_cache_ttl_seconds))

    now = time.time()
    with _cache_lock:
        if _cache_value is not None and now < _cache_expires_at:
            return True, {"cached": True, "snapshot": _cache_value}

    try:
        snap = introspect_schema_snapshot()
    except OperationalError as e:
        return False, {"error": str(e)}

    with _cache_lock:
        _cache_value = snap
        _cache_expires_at = now + ttl
    return True, {"cached": False, "snapshot": snap}


def format_schema_prompt(snapshot: SchemaSnapshot) -> str:
    """
    Prompt material: tables+columns, FK relationships, and optional descriptions.
    """
    lines: List[str] = []
    lines.append("## Database schema")

    if snapshot.excluded_tables:
        lines.append("")
        lines.append("### Exposure rules")
        lines.append("The following tables were excluded by allowlist/denylist:")
        for t in snapshot.excluded_tables:
            lines.append(f"- {t}")

    lines.append("")
    lines.append("### Tables")
    for t in snapshot.tables:
        lines.append(f"- {t.schema}.{t.name}")
        if t.description:
            lines.append(f"  - description: {t.description}")
        for c in t.columns:
            nullable = "" if not c.is_nullable else " NULL"
            lines.append(f"  - {c.name}: {c.data_type}{nullable}")

    lines.append("")
    lines.append("### Foreign keys")
    if not snapshot.foreign_keys:
        lines.append("(none)")
    else:
        for fk in snapshot.foreign_keys:
            src_cols = ", ".join(fk.src_columns)
            dst_cols = ", ".join(fk.dst_columns)
            lines.append(
                f"- {fk.src_schema}.{fk.src_table}({src_cols}) -> {fk.dst_schema}.{fk.dst_table}({dst_cols}) "
                f"[{fk.constraint_name}]"
            )

    return "\n".join(lines).strip() + "\n"
