# modules/functions/database_manager.py
# -*- coding: utf-8 -*-

"""
SQLite manager for persisting precomputed routing data (minimal schema).

Table
-----
CREATE TABLE IF NOT EXISTS heatmap_runs (
      unique_id                 INTEGER PRIMARY KEY AUTOINCREMENT
    , origin_name               TEXT        NOT NULL
    , origin_lat                REAL
    , origin_lon                REAL
    , destiny_name              TEXT        NOT NULL
    , destiny_lat               REAL
    , destiny_lon               REAL

    , road_only_distance_km     REAL

    , cab_po_name               TEXT
    , cab_pd_name               TEXT

    , cab_road_o_to_po_km       REAL
    , cab_road_pd_to_d_km       REAL

    , is_hgv                    BOOL
    , insertion_timestamp       TIMESTAMP   NOT NULL DEFAULT (datetime('now'))
);

Notes
-----
• Coordinates are allowed to be NULL for geocoding failures or placeholder rows.
• Uniqueness enforced via a UNIQUE INDEX on (origin_name, destiny_name)
  so ON CONFLICT upsert works while keeping `unique_id` as rowid PK.
• Only the fields needed to re-derive KPIs are stored.

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
DEFAULT_TABLE   = "heatmap_runs"

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
      unique_id                 INTEGER PRIMARY KEY AUTOINCREMENT
    , origin_name               TEXT        NOT NULL
    , origin_lat                REAL
    , origin_lon                REAL
    , destiny_name              TEXT        NOT NULL
    , destiny_lat               REAL
    , destiny_lon               REAL

    , road_only_distance_km     REAL

    , cab_po_name               TEXT
    , cab_pd_name               TEXT

    , cab_road_o_to_po_km       REAL
    , cab_road_pd_to_d_km       REAL

    , is_hgv                    BOOL
    , insertion_timestamp       TIMESTAMP   NOT NULL DEFAULT (datetime('now'))
);
""".strip()

_CREATE_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_{table}_key
    ON {table} (origin_name, destiny_name);
""".strip()

_CREATE_IDX_DEST_SQL = """
CREATE INDEX IF NOT EXISTS idx_{table}_destiny
    ON {table} (destiny_name);
""".strip()


def ensure_main_table(
    conn: sqlite3.Connection
    , *
    , table_name: str = DEFAULT_TABLE
) -> None:
    """
    Create the main table + unique index if not present.
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
    , origin_name: str
    , origin_lat: Optional[float]
    , origin_lon: Optional[float]
    , destiny_name: str
    , destiny_lat: Optional[float]
    , destiny_lon: Optional[float]
    , road_only_distance_km: Optional[float] = None
    , cab_po_name: Optional[str] = None
    , cab_pd_name: Optional[str] = None
    , cab_road_o_to_po_km: Optional[float] = None
    , cab_road_pd_to_d_km: Optional[float] = None
    , is_hgv: Optional[bool] = None
    , table_name: str = DEFAULT_TABLE
) -> None:
    """
    Insert or update a row keyed by (origin_name, destiny_name).
    Coordinates and distances may be NULL (e.g., geocode failures).
    insertion_timestamp is left unchanged on updates.
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
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(origin_name, destiny_name) DO UPDATE SET
          origin_lat            = excluded.origin_lat
        , origin_lon            = excluded.origin_lon
        , destiny_lat           = excluded.destiny_lat
        , destiny_lon           = excluded.destiny_lon
        , road_only_distance_km = excluded.road_only_distance_km
        , cab_po_name           = excluded.cab_po_name
        , cab_pd_name           = excluded.cab_pd_name
        , cab_road_o_to_po_km   = excluded.cab_road_o_to_po_km
        , cab_road_pd_to_d_km   = excluded.cab_road_pd_to_d_km
        , is_hgv                = excluded.is_hgv
    ;
    """.strip()

    params = (
          origin_name
        , _to_float_or_none(origin_lat)
        , _to_float_or_none(origin_lon)
        , destiny_name
        , _to_float_or_none(destiny_lat)
        , _to_float_or_none(destiny_lon)
        , _to_float_or_none(road_only_distance_km)
        , cab_po_name
        , cab_pd_name
        , _to_float_or_none(cab_road_o_to_po_km)
        , _to_float_or_none(cab_road_pd_to_d_km)
        , _bool_to_int(is_hgv)
    )
    conn.execute(sql, params)


def insert_if_absent(
    conn: sqlite3.Connection
    , *
    , origin_name: str
    , origin_lat: Optional[float]
    , origin_lon: Optional[float]
    , destiny_name: str
    , destiny_lat: Optional[float]
    , destiny_lon: Optional[float]
    , road_only_distance_km: Optional[float] = None
    , cab_po_name: Optional[str] = None
    , cab_pd_name: Optional[str] = None
    , cab_road_o_to_po_km: Optional[float] = None
    , cab_road_pd_to_d_km: Optional[float] = None
    , is_hgv: Optional[bool] = None
    , table_name: str = DEFAULT_TABLE
) -> bool:
    """
    Insert only; ignore if (origin_name, destiny_name) exists.
    Returns True if a row was inserted, False otherwise.
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
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """.strip()

    cur = conn.execute(sql, (
          origin_name
        , _to_float_or_none(origin_lat)
        , _to_float_or_none(origin_lon)
        , destiny_name
        , _to_float_or_none(destiny_lat)
        , _to_float_or_none(destiny_lon)
        , _to_float_or_none(road_only_distance_km)
        , cab_po_name
        , cab_pd_name
        , _to_float_or_none(cab_road_o_to_po_km)
        , _to_float_or_none(cab_road_pd_to_d_km)
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
    Bulk upsert rows. Each row dict must provide the same keys
    accepted by upsert_run (names only; types are relaxed).
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
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(origin_name, destiny_name) DO UPDATE SET
          origin_lat            = excluded.origin_lat
        , origin_lon            = excluded.origin_lon
        , destiny_lat           = excluded.destiny_lat
        , destiny_lon           = excluded.destiny_lon
        , road_only_distance_km = excluded.road_only_distance_km
        , cab_po_name           = excluded.cab_po_name
        , cab_pd_name           = excluded.cab_pd_name
        , cab_road_o_to_po_km   = excluded.cab_road_o_to_po_km
        , cab_road_pd_to_d_km   = excluded.cab_road_pd_to_d_km
        , is_hgv                = excluded.is_hgv
    ;
    """.strip()

    def _row_to_params(
        r: Mapping[str, Any]
    ) -> Tuple[Any, ...]:
        return (
              r["origin_name"]
            , _to_float_or_none(r.get("origin_lat"))
            , _to_float_or_none(r.get("origin_lon"))
            , r["destiny_name"]
            , _to_float_or_none(r.get("destiny_lat"))
            , _to_float_or_none(r.get("destiny_lon"))
            , _to_float_or_none(r.get("road_only_distance_km"))
            , r.get("cab_po_name")
            , r.get("cab_pd_name")
            , _to_float_or_none(r.get("cab_road_o_to_po_km"))
            , _to_float_or_none(r.get("cab_road_pd_to_d_km"))
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
    , keys: Sequence[Tuple[str, str]]
    , rows: Iterable[Mapping[str, Any]]
    , table_name: str = DEFAULT_TABLE
) -> int:
    """
    Overwrite semantics for a subset of composite keys:
        1) delete keys provided
        2) bulk-upsert replacement rows

    keys = sequence of (origin_name, destiny_name)
    """
    ensure_main_table(conn, table_name=table_name)

    del_sql = f"""
    DELETE FROM {table_name}
    WHERE origin_name = ?
      AND destiny_name = ?;
    """.strip()

    if keys:
        conn.executemany(del_sql, [ (k[0], k[1]) for k in keys ])

    return bulk_upsert_runs(conn, rows=rows, table_name=table_name)


def get_run(
    conn: sqlite3.Connection
    , *
    , origin_name: str
    , destiny_name: str
    , table_name: str = DEFAULT_TABLE
) -> Optional[Mapping[str, Any]]:
    """
    Fetch a single row by the composite key. Returns dict or None.
    """
    ensure_main_table(conn, table_name=table_name)

    sql = f"""
    SELECT
          unique_id
        , origin_name
        , origin_lat
        , origin_lon
        , destiny_name
        , destiny_lat
        , destiny_lon
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
        , insertion_timestamp
    FROM {table_name}
    WHERE origin_name = ?
      AND destiny_name = ?;
    """.strip()

    row = conn.execute(sql, (origin_name, destiny_name)).fetchone()
    if not row:
        return None

    (
          unique_id
        , origin_name_v
        , origin_lat_v
        , origin_lon_v
        , destiny_name_v
        , destiny_lat_v
        , destiny_lon_v
        , road_only_distance_km_v
        , cab_po_name_v
        , cab_pd_name_v
        , cab_road_o_to_po_km_v
        , cab_road_pd_to_d_km_v
        , is_hgv_v
        , insertion_timestamp_v
    ) = row

    return {
          "unique_id": unique_id
        , "origin_name": origin_name_v
        , "origin_lat": _to_float_or_none(origin_lat_v)
        , "origin_lon": _to_float_or_none(origin_lon_v)
        , "destiny_name": destiny_name_v
        , "destiny_lat": _to_float_or_none(destiny_lat_v)
        , "destiny_lon": _to_float_or_none(destiny_lon_v)
        , "road_only_distance_km": _to_float_or_none(road_only_distance_km_v)
        , "cab_po_name": cab_po_name_v
        , "cab_pd_name": cab_pd_name_v
        , "cab_road_o_to_po_km": _to_float_or_none(cab_road_o_to_po_km_v)
        , "cab_road_pd_to_d_km": _to_float_or_none(cab_road_pd_to_d_km_v)
        , "is_hgv": (None if is_hgv_v is None else bool(is_hgv_v))
        , "insertion_timestamp": insertion_timestamp_v
    }


def list_runs(
    conn: sqlite3.Connection
    , *
    , origin_name: Optional[str] = None
    , destiny_name: Optional[str] = None
    , table_name: str = DEFAULT_TABLE
    , limit: Optional[int] = None
) -> list[Mapping[str, Any]]:
    """
    List rows with optional filters. Useful for sanity checks / exports.
    """
    ensure_main_table(conn, table_name=table_name)

    clauses: list[str] = []
    params:  list[Any] = []

    if origin_name is not None:
        clauses.append("origin_name = ?")
        params.append(origin_name)

    if destiny_name is not None:
        clauses.append("destiny_name = ?")
        params.append(destiny_name)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    lim   = f" LIMIT {int(limit)}" if (limit is not None and limit > 0) else ""

    sql = f"""
    SELECT
          unique_id
        , origin_name
        , origin_lat
        , origin_lon
        , destiny_name
        , destiny_lat
        , destiny_lon
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
        , insertion_timestamp
    FROM {table_name}
    {where}
    ORDER BY origin_name, destiny_name
    {lim};
    """.strip()

    out: list[Mapping[str, Any]] = []
    for row in conn.execute(sql, tuple(params)).fetchall():
        out.append({
              "unique_id": row[0]
            , "origin_name": row[1]
            , "origin_lat": _to_float_or_none(row[2])
            , "origin_lon": _to_float_or_none(row[3])
            , "destiny_name": row[4]
            , "destiny_lat": _to_float_or_none(row[5])
            , "destiny_lon": _to_float_or_none(row[6])
            , "road_only_distance_km": _to_float_or_none(row[7])
            , "cab_po_name": row[8]
            , "cab_pd_name": row[9]
            , "cab_road_o_to_po_km": _to_float_or_none(row[10])
            , "cab_road_pd_to_d_km": _to_float_or_none(row[11])
            , "is_hgv": (None if row[12] is None else bool(row[12]))
            , "insertion_timestamp": row[13]
        })
    return out


def delete_key(
    conn: sqlite3.Connection
    , *
    , origin_name: str
    , destiny_name: str
    , table_name: str = DEFAULT_TABLE
) -> int:
    """
    Delete a single composite key. Returns affected row count (0 or 1).
    """
    ensure_main_table(conn, table_name=table_name)
    cur = conn.execute(
          f"DELETE FROM {table_name} WHERE origin_name=? AND destiny_name=?"
        , (origin_name, destiny_name)
    )
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
            , origin_name="Avenida Professor Luciano Gualberto, São Paulo, Brazil"
            , origin_lat=-23.558808
            , origin_lon=-46.730357
            , destiny_name="Itapoá, SC, Brazil"
            , destiny_lat=-26.171181
            , destiny_lon=-48.600218
            , road_only_distance_km=612.3
            , cab_po_name="Santos"
            , cab_pd_name="Itajaí"
            , cab_road_o_to_po_km=82.0
            , cab_road_pd_to_d_km=76.0
            , is_hgv=True
        )
        log.info("Rows (limit 3): %s", list_runs(_conn, limit=3))
