# modules/functions/database_manager.py
# -*- coding: utf-8 -*-

"""
SQLite manager for persisting precomputed *road* routing data (generic O→D cache).

Table
-----
CREATE TABLE IF NOT EXISTS heatmap_runs (
      origin              TEXT      NOT NULL
    , origin_lat          REAL
    , origin_lon          REAL
    , destiny             TEXT      NOT NULL
    , destiny_lat         REAL
    , destiny_lon         REAL
    , distance_km         REAL
    , is_hgv              INTEGER   -- 1 = HGV profile, 0 = non-HGV, NULL = unspecified
    , insertion_timestamp TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

Notes
-----
• This is a *generic road legs cache*:
    (origin, destiny, is_hgv) → distance_km + coordinates.

  It is meant to be reused across:
    - road-only O→D legs
    - cabotage legs (O→Po, Pd→D)
    - any other ORS directions calls.

• Coordinates and distance are allowed to be NULL for geocoding failures
  or placeholder rows (e.g. you want to mark "we tried and failed").

• Uniqueness is enforced via a UNIQUE INDEX on (origin, destiny, is_hgv),
  so ON CONFLICT upsert works without a manual PK column.

Style
-----
• 4-space indentation
• comma-at-beginning for multi-line argument lists
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple

# ────────────────────────────────────────────────────────────────────────────────
# Defaults & logger
# ────────────────────────────────────────────────────────────────────────────────

DEFAULT_DB_PATH = Path("data/database/carbon_footprint.sqlite")
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
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")


def connect(
    db_path: Path | str = DEFAULT_DB_PATH
) -> sqlite3.Connection:
    path = Path(db_path)
    _ensure_parent_dir(path)
    conn = sqlite3.connect(path.as_posix())
    _configure_pragmas(conn)
    return conn


@contextmanager
def db_session(
    db_path: Path | str = DEFAULT_DB_PATH
):
    """
    Context-managed connection with commit/rollback.
    """
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("SQLite transaction rolled back due to an error.")
        raise
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────────────────────────
# DDL — create table & indexes
# ────────────────────────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
      origin              TEXT      NOT NULL
    , origin_lat          REAL
    , origin_lon          REAL
    , destiny             TEXT      NOT NULL
    , destiny_lat         REAL
    , destiny_lon         REAL
    , distance_km         REAL
    , is_hgv              INTEGER
    , insertion_timestamp TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
""".strip()

_CREATE_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_{table}_key
    ON {table} (origin, destiny, is_hgv);
""".strip()

_CREATE_IDX_DEST_SQL = """
CREATE INDEX IF NOT EXISTS idx_{table}_destiny
    ON {table} (destiny);
""".strip()


def ensure_main_table(
    conn: sqlite3.Connection
    , *
    , table_name: str = DEFAULT_TABLE
) -> None:
    """
    Create the main table + indexes if not present.
    """
    conn.execute(_CREATE_TABLE_SQL.format(table=table_name))
    conn.execute(_CREATE_UNIQUE_INDEX_SQL.format(table=table_name))
    conn.execute(_CREATE_IDX_DEST_SQL.format(table=table_name))


# ────────────────────────────────────────────────────────────────────────────────
# DML — upsert / insert-only / overwrite / reads
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
    Insert or update a *road leg* keyed by (origin, destiny, is_hgv).

    Coordinates and distances may be NULL (e.g., geocode failures).

    insertion_timestamp:
        - set automatically on INSERT via DEFAULT (datetime('now'))
        - left unchanged on UPDATE (we don't touch it in the UPSERT clause)
    """
    ensure_main_table(conn, table_name=table_name)

    sql = f"""
    INSERT INTO {table_name} (
          origin
        , origin_lat
        , origin_lon
        , destiny
        , destiny_lat
        , destiny_lon
        , distance_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(origin, destiny, is_hgv) DO UPDATE SET
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
    Insert only; ignore if (origin, destiny, is_hgv) already exists.

    Returns True if a row was inserted, False otherwise.
    """
    ensure_main_table(conn, table_name=table_name)

    sql = f"""
    INSERT OR IGNORE INTO {table_name} (
          origin
        , origin_lat
        , origin_lon
        , destiny
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
    Bulk upsert rows.

    Each row dict must provide keys compatible with `upsert_run`:
      origin, origin_lat, origin_lon, destiny, destiny_lat, destiny_lon,
      distance_km (optional), is_hgv (optional).
    """
    ensure_main_table(conn, table_name=table_name)

    sql = f"""
    INSERT INTO {table_name} (
          origin
        , origin_lat
        , origin_lon
        , destiny
        , destiny_lat
        , destiny_lon
        , distance_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(origin, destiny, is_hgv) DO UPDATE SET
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

    params = [ _row_to_params(r) for r in rows ]
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
    """
    ensure_main_table(conn, table_name=table_name)

    del_sql_null = f"""
    DELETE FROM {table_name}
    WHERE origin  = ?
      AND destiny = ?
      AND is_hgv IS NULL;
    """.strip()

    del_sql_bool = f"""
    DELETE FROM {table_name}
    WHERE origin  = ?
      AND destiny = ?
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
    Fetch a single row by (origin, destiny, is_hgv). Returns dict or None.

    If is_hgv is None, this looks for the NULL-profile row.
    """
    ensure_main_table(conn, table_name=table_name)

    if is_hgv is None:
        sql = f"""
        SELECT
              origin
            , origin_lat
            , origin_lon
            , destiny
            , destiny_lat
            , destiny_lon
            , distance_km
            , is_hgv
            , insertion_timestamp
        FROM {table_name}
        WHERE origin  = ?
          AND destiny = ?
          AND is_hgv IS NULL;
        """.strip()
        params: Tuple[Any, ...] = (origin, destiny)
    else:
        sql = f"""
        SELECT
              origin
            , origin_lat
            , origin_lon
            , destiny
            , destiny_lat
            , destiny_lon
            , distance_km
            , is_hgv
            , insertion_timestamp
        FROM {table_name}
        WHERE origin  = ?
          AND destiny = ?
          AND is_hgv = ?;
        """.strip()
        params = (origin, destiny, _bool_to_int(is_hgv))

    row = conn.execute(sql, params).fetchone()
    if not row:
        return None

    (
          origin_v
        , origin_lat_v
        , origin_lon_v
        , destiny_v
        , destiny_lat_v
        , destiny_lon_v
        , distance_km_v
        , is_hgv_v
        , insertion_timestamp_v
    ) = row

    return {
          "origin": origin_v
        , "origin_lat": _to_float_or_none(origin_lat_v)
        , "origin_lon": _to_float_or_none(origin_lon_v)
        , "destiny": destiny_v
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
    """
    ensure_main_table(conn, table_name=table_name)

    clauses: list[str] = []
    params:  list[Any] = []

    if origin is not None:
        clauses.append("origin = ?")
        params.append(origin)

    if destiny is not None:
        clauses.append("destiny = ?")
        params.append(destiny)

    if is_hgv is not None:
        clauses.append("is_hgv = ?")
        params.append(_bool_to_int(is_hgv))

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    lim   = f" LIMIT {int(limit)}" if (limit is not None and limit > 0) else ""

    sql = f"""
    SELECT
          origin
        , origin_lat
        , origin_lon
        , destiny
        , destiny_lat
        , destiny_lon
        , distance_km
        , is_hgv
        , insertion_timestamp
    FROM {table_name}
    {where}
    ORDER BY origin, destiny, is_hgv
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

    If is_hgv is None, deletes all rows for (origin, destiny),
    regardless of profile.
    """
    ensure_main_table(conn, table_name=table_name)

    if is_hgv is None:
        sql = f"""
        DELETE FROM {table_name}
        WHERE origin  = ?
          AND destiny = ?;
        """.strip()
        params = (origin, destiny)
    else:
        sql = f"""
        DELETE FROM {table_name}
        WHERE origin  = ?
          AND destiny = ?
          AND is_hgv = ?;
        """.strip()
        params = (origin, destiny, _bool_to_int(is_hgv))

    cur = conn.execute(sql, params)
    return cur.rowcount


# ────────────────────────────────────────────────────────────────────────────────
# CLI smoke (optional)
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with db_session() as _conn:
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
        log.info("Rows (limit 3): %s", list_runs(_conn, limit=3))
