# modules/infra/database_manager.py
# -*- coding: utf-8 -*-
"""
SQLite manager for routing cache + multimodal fuel/emissions results
====================================================================

This module now handles two generic families of tables:

1) Road legs cache (existing behavior, default table = "routes")
   ----------------------------------------------------------------
   Schema (per table):

       CREATE TABLE IF NOT EXISTS {table} (
             origin_name         TEXT      NOT NULL
           , origin_lat          REAL
           , origin_lon          REAL
           , destiny_name        TEXT      NOT NULL
           , destiny_lat         REAL
           , destiny_lon         REAL
           , distance_km         REAL
           , is_hgv              INTEGER   -- 1 = HGV profile, 0 = non-HGV, NULL = unspecified
           , insertion_timestamp TIMESTAMP NOT NULL DEFAULT (datetime('now'))
       );

   Unique index on (origin_name, destiny_name, is_hgv).

   This is a *generic road legs cache*:
     (origin_name, destiny_name, is_hgv) → distance_km + coordinates.

   It is meant to be reused across:
     - road-only O→D legs
     - cabotage legs (O→Po, Pd→D)
     - any other ORS directions calls.

2) Multimodal results tables (NEW, generic name per origin/amount)
   ----------------------------------------------------------------
   Schema (per table):

       CREATE TABLE IF NOT EXISTS {table} (
             origin_name             TEXT      NOT NULL
           , destiny_name            TEXT      NOT NULL
           , cargo_t                 REAL      NOT NULL
           , road_distance_km        REAL
           , road_fuel_liters        REAL
           , road_fuel_kg            REAL
           , road_fuel_cost_r        REAL
           , road_co2e_kg            REAL
           , mm_road_fuel_liters     REAL
           , mm_road_fuel_kg         REAL
           , mm_road_fuel_cost_r     REAL
           , mm_road_co2e_kg         REAL
           , sea_km                  REAL
           , sea_fuel_kg             REAL
           , sea_fuel_cost_r         REAL
           , sea_co2e_kg             REAL
           , total_fuel_kg           REAL
           , total_fuel_cost_r       REAL
           , total_co2e_kg           REAL
           , delta_cost_r            REAL
           , delta_co2e_kg           REAL
           , insertion_timestamp     TIMESTAMP NOT NULL DEFAULT (datetime('now'))
       );

   Unique index on (destiny_name).

   Intended usage:
     - One table per (origin, cargo_t) run, with a name such as:
         "Sao_Paulo__26tons"
     - Each row = one destination with full road×multimodal metrics.

Style
-----
• 4-space indentation
• comma-at-beginning for multi-line argument lists
"""

from __future__ import annotations

import logging
import json
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple, List

from modules.core.types import Path

# ────────────────────────────────────────────────────────────────────────────────
# Defaults & logger
# ────────────────────────────────────────────────────────────────────────────────

DEFAULT_DB_PATH = Path("data/processed/database/carbon_footprint.sqlite")
DEFAULT_TABLE   = "routes"

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# Small helpers
# ────────────────────────────────────────────────────────────────────────────────

def _bool_to_int(
    v: Optional[bool]
) -> Optional[int]:
    """
    Convert a bool to 0/1, preserving None.
    """
    if v is None:
        return None
    return 1 if bool(v) else 0


def _int_to_bool(
    v: Any
) -> Optional[bool]:
    """
    Convert DB integer (0/1/NULL) back to bool/None.
    """
    if v is None:
        return None
    return bool(v)


def _to_float_or_none(
    v: Any
) -> Optional[float]:
    """
    Safely convert to float, keeping None (and "") as None.
    Accepts str/float/int/None.
    """
    if v is None or v == "":
        return None
    return float(v)


# ────────────────────────────────────────────────────────────────────────────────
# Connection helpers
# ────────────────────────────────────────────────────────────────────────────────

def _ensure_parent_dir(
    db_path: Path
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _configure_pragmas(
    conn: sqlite3.Connection
) -> None:
    """
    Apply pragmatic defaults for a small local analytical DB.
    """
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")


def connect(
    db_path: Path | str = DEFAULT_DB_PATH
) -> sqlite3.Connection:
    """
    Open a SQLite connection with parent folder + PRAGMAs taken care of.
    """
    path = Path(db_path)
    _ensure_parent_dir(path)
    conn = sqlite3.connect(path.as_posix())
    _configure_pragmas(conn)
    return conn


@contextmanager
def db_session(db_path: Path | str):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        yield conn
        conn.commit()
    except Exception:
        log.error("SQLite transaction rolled back due to an error.", exc_info=True)
        conn.rollback()
        raise
    finally:
        conn.close()

def upsert_multimodal_payload(
      db_path: Path | str
    , origin_raw: str
    , destiny_raw: str
    , cargo_t: float
    , payload: Mapping[str, Any]
    , table_name: str = "multimodal_results"
) -> None:
    """
    Store the full multimodal JSON payload into a dedicated results table.

    Important:
      • This does NOT touch the road-legs cache table (usually 'routes').
      • The table is keyed by (origin_raw, destiny_raw, cargo_t).
    """

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
          origin_raw    TEXT NOT NULL
        , destiny_raw   TEXT NOT NULL
        , cargo_t       REAL NOT NULL
        , payload_json  TEXT NOT NULL
        , inserted_at   TEXT NOT NULL DEFAULT (datetime('now'))
        , UNIQUE(origin_raw, destiny_raw, cargo_t)
    );
    """

    upsert_sql = f"""
    INSERT INTO {table_name} (
          origin_raw
        , destiny_raw
        , cargo_t
        , payload_json
    )
    VALUES (?, ?, ?, ?)
    ON CONFLICT(origin_raw, destiny_raw, cargo_t)
    DO UPDATE SET
          payload_json = excluded.payload_json
        , inserted_at  = datetime('now');
    """

    payload_json = json.dumps(payload, ensure_ascii=False)

    with db_session(db_path) as conn:
        conn.execute(create_sql)
        conn.execute(
              upsert_sql
            , (origin_raw, destiny_raw, float(cargo_t), payload_json)
        )


# ────────────────────────────────────────────────────────────────────────────────
# DDL — road legs cache (existing behavior)
# ────────────────────────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
      origin_name         TEXT      NOT NULL
    , origin_lat          REAL
    , origin_lon          REAL
    , destiny_name        TEXT      NOT NULL
    , destiny_lat         REAL
    , destiny_lon         REAL
    , distance_km         REAL
    , is_hgv              INTEGER
    , insertion_timestamp TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
""".strip()

_CREATE_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_{table}_key
    ON {table} (origin_name, destiny_name, is_hgv);
""".strip()

_CREATE_IDX_DEST_SQL = """
CREATE INDEX IF NOT EXISTS idx_{table}_destiny_name
    ON {table} (destiny_name);
""".strip()


def ensure_main_table(
    conn: sqlite3.Connection
    , *
    , table_name: str = DEFAULT_TABLE
) -> None:
    """
    Create the main road-legs table + indexes if not present.

    Parameters
    ----------
    table_name
        Name of the road-legs table to manage (default: "routes").
    """
    conn.execute(_CREATE_TABLE_SQL.format(table=table_name))
    conn.execute(_CREATE_UNIQUE_INDEX_SQL.format(table=table_name))
    conn.execute(_CREATE_IDX_DEST_SQL.format(table=table_name))


# ────────────────────────────────────────────────────────────────────────────────
# DML — road legs cache (upsert / insert-only / reads)
# ────────────────────────────────────────────────────────────────────────────────

def upsert_run(
    conn: sqlite3.Connection
    , *
    , origin: str
    , origin_lat: Optional[float]
    , origin_lon: Optional[float]
    , destiny: str
    , destiny_lat: Optional[float]
    , destiny_lon: Optional[float]
    , distance_km: Optional[float] = None
    , is_hgv: Optional[bool] = None
    , table_name: str = DEFAULT_TABLE
) -> None:
    """
    Insert or update a *road leg* keyed by (origin_name, destiny_name, is_hgv).

    Parameters
    ----------
    origin / destiny
        Human-readable names. They are stored in columns
        origin_name / destiny_name in the database.

    Coordinates and distances may be NULL (e.g., geocode failures).

    insertion_timestamp:
        - set automatically on INSERT via DEFAULT (datetime('now'))
        - left unchanged on UPDATE (we don't touch it in the UPSERT clause)
    """
    ensure_main_table(conn, table_name=table_name)

    sql = f"""
    INSERT INTO {table_name} (
          origin_name
        , origin_lat
        , origin_lon
        , destiny_name
        , destiny_lat
        , destiny_lon
        , distance_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(origin_name, destiny_name, is_hgv) DO UPDATE SET
          origin_lat   = excluded.origin_lat
        , origin_lon   = excluded.origin_lon
        , destiny_lat  = excluded.destiny_lat
        , destiny_lon  = excluded.destiny_lon
        , distance_km  = excluded.distance_km
        , is_hgv       = excluded.is_hgv
    ;
    """.strip()

    params = (
          origin
        , _to_float_or_none(origin_lat)
        , _to_float_or_none(origin_lon)
        , destiny
        , _to_float_or_none(destiny_lat)
        , _to_float_or_none(destiny_lon)
        , _to_float_or_none(distance_km)
        , _bool_to_int(is_hgv)
    )
    conn.execute(sql, params)


def insert_if_absent(
    conn: sqlite3.Connection
    , *
    , origin: str
    , origin_lat: Optional[float]
    , origin_lon: Optional[float]
    , destiny: str
    , destiny_lat: Optional[float]
    , destiny_lon: Optional[float]
    , distance_km: Optional[float] = None
    , is_hgv: Optional[bool] = None
    , table_name: str = DEFAULT_TABLE
) -> bool:
    """
    Insert only; ignore if (origin_name, destiny_name, is_hgv) already exists.

    Returns
    -------
    bool
        True if a row was inserted, False otherwise.
    """
    ensure_main_table(conn, table_name=table_name)

    sql = f"""
    INSERT OR IGNORE INTO {table_name} (
          origin_name
        , origin_lat
        , origin_lon
        , destiny_name
        , destiny_lat
        , destiny_lon
        , distance_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """.strip()

    cur = conn.execute(sql, (
          origin
        , _to_float_or_none(origin_lat)
        , _to_float_or_none(origin_lon)
        , destiny
        , _to_float_or_none(destiny_lat)
        , _to_float_or_none(destiny_lon)
        , _to_float_or_none(distance_km)
        , _bool_to_int(is_hgv)
    ))
    return cur.rowcount == 1


def bulk_upsert_runs(
    conn: sqlite3.Connection
    , *
    , rows: Iterable[Mapping[str, Any]]
    , table_name: str = DEFAULT_TABLE
) -> int:
    """
    Bulk upsert rows for a road-legs table.

    Each row dict must provide keys compatible with `upsert_run`:
      origin, origin_lat, origin_lon, destiny, destiny_lat, destiny_lon,
      distance_km (optional), is_hgv (optional).

    They are stored in DB columns origin_name / destiny_name.
    """
    ensure_main_table(conn, table_name=table_name)

    sql = f"""
    INSERT INTO {table_name} (
          origin_name
        , origin_lat
        , origin_lon
        , destiny_name
        , destiny_lat
        , destiny_lon
        , distance_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(origin_name, destiny_name, is_hgv) DO UPDATE SET
          origin_lat   = excluded.origin_lat
        , origin_lon   = excluded.origin_lon
        , destiny_lat  = excluded.destiny_lat
        , destiny_lon  = excluded.destiny_lon
        , distance_km  = excluded.distance_km
        , is_hgv       = excluded.is_hgv
    ;
    """.strip()

    def _row_to_params(
        r: Mapping[str, Any]
    ) -> Tuple[Any, ...]:
        return (
              r["origin"]
            , _to_float_or_none(r.get("origin_lat"))
            , _to_float_or_none(r.get("origin_lon"))
            , r["destiny"]
            , _to_float_or_none(r.get("destiny_lat"))
            , _to_float_or_none(r.get("destiny_lon"))
            , _to_float_or_none(r.get("distance_km"))
            , _bool_to_int(r.get("is_hgv"))
        )

    params = [_row_to_params(r) for r in rows]
    if not params:
        return 0

    conn.executemany(sql, params)
    return len(params)


def overwrite_keys(
    conn: sqlite3.Connection
    , *
    , keys: Sequence[Tuple[str, str, Optional[bool]]]
    , rows: Iterable[Mapping[str, Any]]
    , table_name: str = DEFAULT_TABLE
) -> int:
    """
    Overwrite semantics for a subset of composite keys:

        1) delete keys provided
        2) bulk-upsert replacement rows

    keys = sequence of (origin, destiny, is_hgv)
           (is_hgv can be None to target the NULL-profile row).

    In the DB these map to origin_name / destiny_name columns.
    """
    ensure_main_table(conn, table_name=table_name)

    del_sql_null = f"""
    DELETE FROM {table_name}
    WHERE origin_name  = ?
      AND destiny_name = ?
      AND is_hgv IS NULL;
    """.strip()

    del_sql_bool = f"""
    DELETE FROM {table_name}
    WHERE origin_name  = ?
      AND destiny_name = ?
      AND is_hgv = ?;
    """.strip()

    batch_null: list[tuple[Any, ...]] = []
    batch_bool: list[tuple[Any, ...]] = []

    for origin_v, destiny_v, is_hgv_v in keys:
        if is_hgv_v is None:
            batch_null.append((origin_v, destiny_v))
        else:
            batch_bool.append((origin_v, destiny_v, _bool_to_int(is_hgv_v)))

    if batch_null:
        conn.executemany(del_sql_null, batch_null)
    if batch_bool:
        conn.executemany(del_sql_bool, batch_bool)

    return bulk_upsert_runs(conn, rows=rows, table_name=table_name)


def get_run(
    conn: sqlite3.Connection
    , *
    , origin: str
    , destiny: str
    , is_hgv: Optional[bool] = None
    , table_name: str = DEFAULT_TABLE
) -> Optional[Mapping[str, Any]]:
    """
    Fetch a single row by (origin_name, destiny_name, is_hgv). Returns dict or None.

    If is_hgv is None, this looks for the NULL-profile row.
    """
    ensure_main_table(conn, table_name=table_name)

    if is_hgv is None:
        sql = f"""
        SELECT
              origin_name
            , origin_lat
            , origin_lon
            , destiny_name
            , destiny_lat
            , destiny_lon
            , distance_km
            , is_hgv
            , insertion_timestamp
        FROM {table_name}
        WHERE origin_name  = ?
          AND destiny_name = ?
          AND is_hgv IS NULL;
        """.strip()
        params: Tuple[Any, ...] = (origin, destiny)
    else:
        sql = f"""
        SELECT
              origin_name
            , origin_lat
            , origin_lon
            , destiny_name
            , destiny_lat
            , destiny_lon
            , distance_km
            , is_hgv
            , insertion_timestamp
        FROM {table_name}
        WHERE origin_name  = ?
          AND destiny_name = ?
          AND is_hgv = ?;
        """.strip()
        params = (origin, destiny, _bool_to_int(is_hgv))

    row = conn.execute(sql, params).fetchone()
    if not row:
        return None

    (
          origin_name_v
        , origin_lat_v
        , origin_lon_v
        , destiny_name_v
        , destiny_lat_v
        , destiny_lon_v
        , distance_km_v
        , is_hgv_v
        , insertion_timestamp_v
    ) = row

    return {
          "origin": origin_name_v
        , "origin_lat": _to_float_or_none(origin_lat_v)
        , "origin_lon": _to_float_or_none(origin_lon_v)
        , "destiny": destiny_name_v
        , "destiny_lat": _to_float_or_none(destiny_lat_v)
        , "destiny_lon": _to_float_or_none(destiny_lon_v)
        , "distance_km": _to_float_or_none(distance_km_v)
        , "is_hgv": _int_to_bool(is_hgv_v)
        , "insertion_timestamp": insertion_timestamp_v
    }


def list_runs(
    conn: sqlite3.Connection
    , *
    , origin: Optional[str] = None
    , destiny: Optional[str] = None
    , is_hgv: Optional[bool] = None
    , table_name: str = DEFAULT_TABLE
    , limit: Optional[int] = None
) -> list[Mapping[str, Any]]:
    """
    List rows with optional filters. Useful for sanity checks / exports.

    Semantics:
      - if is_hgv is True/False → filter by that profile
      - if is_hgv is None      → do not filter by profile

    Filters use origin / destiny, but in the DB those map to
    origin_name / destiny_name columns.
    """
    ensure_main_table(conn, table_name=table_name)

    clauses: list[str] = []
    params:  list[Any] = []

    if origin is not None:
        clauses.append("origin_name = ?")
        params.append(origin)

    if destiny is not None:
        clauses.append("destiny_name = ?")
        params.append(destiny)

    if is_hgv is not None:
        clauses.append("is_hgv = ?")
        params.append(_bool_to_int(is_hgv))

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    lim   = f" LIMIT {int(limit)}" if (limit is not None and limit > 0) else ""

    sql = f"""
    SELECT
          origin_name
        , origin_lat
        , origin_lon
        , destiny_name
        , destiny_lat
        , destiny_lon
        , distance_km
        , is_hgv
        , insertion_timestamp
    FROM {table_name}
    {where}
    ORDER BY origin_name, destiny_name, is_hgv
    {lim};
    """.strip()

    out: list[Mapping[str, Any]] = []
    for row in conn.execute(sql, tuple(params)).fetchall():
        out.append({
              "origin": row[0]
            , "origin_lat": _to_float_or_none(row[1])
            , "origin_lon": _to_float_or_none(row[2])
            , "destiny": row[3]
            , "destiny_lat": _to_float_or_none(row[4])
            , "destiny_lon": _to_float_or_none(row[5])
            , "distance_km": _to_float_or_none(row[6])
            , "is_hgv": _int_to_bool(row[7])
            , "insertion_timestamp": row[8]
        })
    return out


def delete_key(
    conn: sqlite3.Connection
    , *
    , origin: str
    , destiny: str
    , is_hgv: Optional[bool] = None
    , table_name: str = DEFAULT_TABLE
) -> int:
    """
    Delete a single composite key. Returns affected row count.

    If is_hgv is None, deletes all rows for (origin_name, destiny_name),
    regardless of profile.
    """
    ensure_main_table(conn, table_name=table_name)

    if is_hgv is None:
        sql = f"""
        DELETE FROM {table_name}
        WHERE origin_name  = ?
          AND destiny_name = ?;
        """.strip()
        params = (origin, destiny)
    else:
        sql = f"""
        DELETE FROM {table_name}
        WHERE origin_name  = ?
          AND destiny_name = ?
          AND is_hgv = ?;
        """.strip()
        params = (origin, destiny, _bool_to_int(is_hgv))

    cur = conn.execute(sql, params)
    return cur.rowcount


# ────────────────────────────────────────────────────────────────────────────────
# DDL — multimodal results tables (NEW)
# ────────────────────────────────────────────────────────────────────────────────

_CREATE_MM_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
      origin_name             TEXT      NOT NULL
    , destiny_name            TEXT      NOT NULL
    , cargo_t                 REAL      NOT NULL
    , road_distance_km        REAL
    , road_fuel_liters        REAL
    , road_fuel_kg            REAL
    , road_fuel_cost_r        REAL
    , road_co2e_kg            REAL
    , mm_road_fuel_liters     REAL
    , mm_road_fuel_kg         REAL
    , mm_road_fuel_cost_r     REAL
    , mm_road_co2e_kg         REAL
    , sea_km                  REAL
    , sea_fuel_kg             REAL
    , sea_fuel_cost_r         REAL
    , sea_co2e_kg             REAL
    , total_fuel_kg           REAL
    , total_fuel_cost_r       REAL
    , total_co2e_kg           REAL
    , delta_cost_r            REAL
    , delta_co2e_kg           REAL
    , insertion_timestamp     TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
""".strip()

_CREATE_MM_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_{table}_destiny
    ON {table} (destiny_name);
""".strip()


def ensure_multimodal_results_table(
    conn: sqlite3.Connection
    , *
    , table_name: str
) -> None:
    """
    Ensure a multimodal results table exists with the standard schema.

    The name is fully configurable, so you can use patterns like:

        origin_tag = "Sao_Paulo"
        amount_tag = "26tons"
        table_name = f"{origin_tag}__{amount_tag}"

        ensure_multimodal_results_table(conn, table_name=table_name)
    """
    conn.execute(_CREATE_MM_TABLE_SQL.format(table=table_name))
    conn.execute(_CREATE_MM_UNIQUE_INDEX_SQL.format(table=table_name))


# ────────────────────────────────────────────────────────────────────────────────
# DML — multimodal results (upsert / bulk / reads)
# ────────────────────────────────────────────────────────────────────────────────

def upsert_multimodal_result(
    conn: sqlite3.Connection
    , *
    , origin_name: str
    , destiny_name: str
    , cargo_t: float
    , road_distance_km: Optional[float]
    , road_fuel_liters: Optional[float]
    , road_fuel_kg: Optional[float]
    , road_fuel_cost_r: Optional[float]
    , road_co2e_kg: Optional[float]
    , mm_road_fuel_liters: Optional[float]
    , mm_road_fuel_kg: Optional[float]
    , mm_road_fuel_cost_r: Optional[float]
    , mm_road_co2e_kg: Optional[float]
    , sea_km: Optional[float]
    , sea_fuel_kg: Optional[float]
    , sea_fuel_cost_r: Optional[float]
    , sea_co2e_kg: Optional[float]
    , total_fuel_kg: Optional[float]
    , total_fuel_cost_r: Optional[float]
    , total_co2e_kg: Optional[float]
    , delta_cost_r: Optional[float]
    , delta_co2e_kg: Optional[float]
    , table_name: str
) -> None:
    """
    Upsert a single multimodal result row keyed by destiny_name.

    Parameters are intentionally explicit to keep callsites self-documenting.
    """
    ensure_multimodal_results_table(conn, table_name=table_name)

    sql = f"""
    INSERT INTO {table_name} (
          origin_name
        , destiny_name
        , cargo_t
        , road_distance_km
        , road_fuel_liters
        , road_fuel_kg
        , road_fuel_cost_r
        , road_co2e_kg
        , mm_road_fuel_liters
        , mm_road_fuel_kg
        , mm_road_fuel_cost_r
        , mm_road_co2e_kg
        , sea_km
        , sea_fuel_kg
        , sea_fuel_cost_r
        , sea_co2e_kg
        , total_fuel_kg
        , total_fuel_cost_r
        , total_co2e_kg
        , delta_cost_r
        , delta_co2e_kg
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(destiny_name) DO UPDATE SET
          origin_name         = excluded.origin_name
        , cargo_t             = excluded.cargo_t
        , road_distance_km    = excluded.road_distance_km
        , road_fuel_liters    = excluded.road_fuel_liters
        , road_fuel_kg        = excluded.road_fuel_kg
        , road_fuel_cost_r    = excluded.road_fuel_cost_r
        , road_co2e_kg        = excluded.road_co2e_kg
        , mm_road_fuel_liters = excluded.mm_road_fuel_liters
        , mm_road_fuel_kg     = excluded.mm_road_fuel_kg
        , mm_road_fuel_cost_r = excluded.mm_road_fuel_cost_r
        , mm_road_co2e_kg     = excluded.mm_road_co2e_kg
        , sea_km              = excluded.sea_km
        , sea_fuel_kg         = excluded.sea_fuel_kg
        , sea_fuel_cost_r     = excluded.sea_fuel_cost_r
        , sea_co2e_kg         = excluded.sea_co2e_kg
        , total_fuel_kg       = excluded.total_fuel_kg
        , total_fuel_cost_r   = excluded.total_fuel_cost_r
        , total_co2e_kg       = excluded.total_co2e_kg
        , delta_cost_r        = excluded.delta_cost_r
        , delta_co2e_kg       = excluded.delta_co2e_kg
    ;
    """.strip()

    params = (
          origin_name
        , destiny_name
        , _to_float_or_none(cargo_t)
        , _to_float_or_none(road_distance_km)
        , _to_float_or_none(road_fuel_liters)
        , _to_float_or_none(road_fuel_kg)
        , _to_float_or_none(road_fuel_cost_r)
        , _to_float_or_none(road_co2e_kg)
        , _to_float_or_none(mm_road_fuel_liters)
        , _to_float_or_none(mm_road_fuel_kg)
        , _to_float_or_none(mm_road_fuel_cost_r)
        , _to_float_or_none(mm_road_co2e_kg)
        , _to_float_or_none(sea_km)
        , _to_float_or_none(sea_fuel_kg)
        , _to_float_or_none(sea_fuel_cost_r)
        , _to_float_or_none(sea_co2e_kg)
        , _to_float_or_none(total_fuel_kg)
        , _to_float_or_none(total_fuel_cost_r)
        , _to_float_or_none(total_co2e_kg)
        , _to_float_or_none(delta_cost_r)
        , _to_float_or_none(delta_co2e_kg)
    )
    conn.execute(sql, params)


def bulk_upsert_multimodal_results(
    conn: sqlite3.Connection
    , *
    , rows: Iterable[Mapping[str, Any]]
    , table_name: str
) -> int:
    """
    Bulk upsert multimodal results.

    Each row dict must provide keys compatible with `upsert_multimodal_result`:

        origin_name, destiny_name, cargo_t,
        road_distance_km, road_fuel_liters, road_fuel_kg, road_fuel_cost_r, road_co2e_kg,
        mm_road_fuel_liters, mm_road_fuel_kg, mm_road_fuel_cost_r, mm_road_co2e_kg,
        sea_km, sea_fuel_kg, sea_fuel_cost_r, sea_co2e_kg,
        total_fuel_kg, total_fuel_cost_r, total_co2e_kg,
        delta_cost_r, delta_co2e_kg
    """
    ensure_multimodal_results_table(conn, table_name=table_name)

    sql = f"""
    INSERT INTO {table_name} (
          origin_name
        , destiny_name
        , cargo_t
        , road_distance_km
        , road_fuel_liters
        , road_fuel_kg
        , road_fuel_cost_r
        , road_co2e_kg
        , mm_road_fuel_liters
        , mm_road_fuel_kg
        , mm_road_fuel_cost_r
        , mm_road_co2e_kg
        , sea_km
        , sea_fuel_kg
        , sea_fuel_cost_r
        , sea_co2e_kg
        , total_fuel_kg
        , total_fuel_cost_r
        , total_co2e_kg
        , delta_cost_r
        , delta_co2e_kg
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(destiny_name) DO UPDATE SET
          origin_name         = excluded.origin_name
        , cargo_t             = excluded.cargo_t
        , road_distance_km    = excluded.road_distance_km
        , road_fuel_liters    = excluded.road_fuel_liters
        , road_fuel_kg        = excluded.road_fuel_kg
        , road_fuel_cost_r    = excluded.road_fuel_cost_r
        , road_co2e_kg        = excluded.road_co2e_kg
        , mm_road_fuel_liters = excluded.mm_road_fuel_liters
        , mm_road_fuel_kg     = excluded.mm_road_fuel_kg
        , mm_road_fuel_cost_r = excluded.mm_road_fuel_cost_r
        , mm_road_co2e_kg     = excluded.mm_road_co2e_kg
        , sea_km              = excluded.sea_km
        , sea_fuel_kg         = excluded.sea_fuel_kg
        , sea_fuel_cost_r     = excluded.sea_fuel_cost_r
        , sea_co2e_kg         = excluded.sea_co2e_kg
        , total_fuel_kg       = excluded.total_fuel_kg
        , total_fuel_cost_r   = excluded.total_fuel_cost_r
        , total_co2e_kg       = excluded.total_co2e_kg
        , delta_cost_r        = excluded.delta_cost_r
        , delta_co2e_kg       = excluded.delta_co2e_kg
    ;
    """.strip()

    def _row_to_params(
        r: Mapping[str, Any]
    ) -> Tuple[Any, ...]:
        return (
              r["origin_name"]
            , r["destiny_name"]
            , _to_float_or_none(r.get("cargo_t"))
            , _to_float_or_none(r.get("road_distance_km"))
            , _to_float_or_none(r.get("road_fuel_liters"))
            , _to_float_or_none(r.get("road_fuel_kg"))
            , _to_float_or_none(r.get("road_fuel_cost_r"))
            , _to_float_or_none(r.get("road_co2e_kg"))
            , _to_float_or_none(r.get("mm_road_fuel_liters"))
            , _to_float_or_none(r.get("mm_road_fuel_kg"))
            , _to_float_or_none(r.get("mm_road_fuel_cost_r"))
            , _to_float_or_none(r.get("mm_road_co2e_kg"))
            , _to_float_or_none(r.get("sea_km"))
            , _to_float_or_none(r.get("sea_fuel_kg"))
            , _to_float_or_none(r.get("sea_fuel_cost_r"))
            , _to_float_or_none(r.get("sea_co2e_kg"))
            , _to_float_or_none(r.get("total_fuel_kg"))
            , _to_float_or_none(r.get("total_fuel_cost_r"))
            , _to_float_or_none(r.get("total_co2e_kg"))
            , _to_float_or_none(r.get("delta_cost_r"))
            , _to_float_or_none(r.get("delta_co2e_kg"))
        )

    params = [_row_to_params(r) for r in rows]
    if not params:
        return 0

    conn.executemany(sql, params)
    return len(params)


def list_multimodal_results(
    conn: sqlite3.Connection
    , *
    , table_name: str
    , limit: Optional[int] = None
) -> List[Mapping[str, Any]]:
    """
    List rows from a multimodal results table.

    Used primarily for sanity checks and for `--resume` logic in bulk scripts.
    """
    ensure_multimodal_results_table(conn, table_name=table_name)

    lim = f" LIMIT {int(limit)}" if (limit is not None and limit > 0) else ""

    sql = f"""
    SELECT
          origin_name
        , destiny_name
        , cargo_t
        , road_distance_km
        , road_fuel_liters
        , road_fuel_kg
        , road_fuel_cost_r
        , road_co2e_kg
        , mm_road_fuel_liters
        , mm_road_fuel_kg
        , mm_road_fuel_cost_r
        , mm_road_co2e_kg
        , sea_km
        , sea_fuel_kg
        , sea_fuel_cost_r
        , sea_co2e_kg
        , total_fuel_kg
        , total_fuel_cost_r
        , total_co2e_kg
        , delta_cost_r
        , delta_co2e_kg
        , insertion_timestamp
    FROM {table_name}
    ORDER BY destiny_name
    {lim};
    """.strip()

    out: List[Mapping[str, Any]] = []
    for row in conn.execute(sql).fetchall():
        out.append({
              "origin_name": row[0]
            , "destiny_name": row[1]
            , "cargo_t": _to_float_or_none(row[2])
            , "road_distance_km": _to_float_or_none(row[3])
            , "road_fuel_liters": _to_float_or_none(row[4])
            , "road_fuel_kg": _to_float_or_none(row[5])
            , "road_fuel_cost_r": _to_float_or_none(row[6])
            , "road_co2e_kg": _to_float_or_none(row[7])
            , "mm_road_fuel_liters": _to_float_or_none(row[8])
            , "mm_road_fuel_kg": _to_float_or_none(row[9])
            , "mm_road_fuel_cost_r": _to_float_or_none(row[10])
            , "mm_road_co2e_kg": _to_float_or_none(row[11])
            , "sea_km": _to_float_or_none(row[12])
            , "sea_fuel_kg": _to_float_or_none(row[13])
            , "sea_fuel_cost_r": _to_float_or_none(row[14])
            , "sea_co2e_kg": _to_float_or_none(row[15])
            , "total_fuel_kg": _to_float_or_none(row[16])
            , "total_fuel_cost_r": _to_float_or_none(row[17])
            , "total_co2e_kg": _to_float_or_none(row[18])
            , "delta_cost_r": _to_float_or_none(row[19])
            , "delta_co2e_kg": _to_float_or_none(row[20])
            , "insertion_timestamp": row[21]
        })
    return out


def delete_multimodal_result(
    conn: sqlite3.Connection
    , *
    , destiny_name: str
    , table_name: str
) -> int:
    """
    Delete a single multimodal result row by destiny_name.

    Returns affected row count.
    """
    ensure_multimodal_results_table(conn, table_name=table_name)

    sql = f"""
    DELETE FROM {table_name}
    WHERE destiny_name = ?;
    """.strip()

    cur = conn.execute(sql, (destiny_name,))
    return cur.rowcount


# ────────────────────────────────────────────────────────────────────────────────
# CLI smoke (optional)
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Small smoke test: create main routes table and one multimodal table
    with db_session() as _conn:
        # Road cache
        ensure_main_table(_conn)
        upsert_run(
              _conn
            , origin="Avenida Professor Luciano Gualberto, São Paulo, Brazil"
            , origin_lat=-23.558808
            , origin_lon=-46.730357
            , destiny="Porto de Santos"
            , destiny_lat=-23.9608
            , destiny_lon=-46.3336
            , distance_km=90.5919
            , is_hgv=True
        )
        log.info("Routes (limit 3): %s", list_runs(_conn, limit=3))

        # Multimodal test table
        mm_table = "SmokeTest__26tons"
        ensure_multimodal_results_table(_conn, table_name=mm_table)
        upsert_multimodal_result(
              _conn
            , origin_name="São Paulo, SP"
            , destiny_name="Curitiba, PR"
            , cargo_t=26.0
            , road_distance_km=405.5
            , road_fuel_liters=271.0
            , road_fuel_kg=227.64
            , road_fuel_cost_r=1627.0
            , road_co2e_kg=726.9
            , mm_road_fuel_liters=231.0
            , mm_road_fuel_kg=194.0
            , mm_road_fuel_cost_r=1029.0
            , mm_road_co2e_kg=619.9
            , sea_km=330.0
            , sea_fuel_kg=80.0
            , sea_fuel_cost_r=500.0
            , sea_co2e_kg=250.0
            , total_fuel_kg=274.0
            , total_fuel_cost_r=1529.0
            , total_co2e_kg=869.9
            , delta_cost_r=-98.0
            , delta_co2e_kg=143.0
            , table_name=mm_table
        )
        log.info("Multimodal rows (%s): %s", mm_table, list_multimodal_results(_conn, table_name=mm_table))
