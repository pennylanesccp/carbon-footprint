# modules/functions/database_manager.py
# -*- coding: utf-8 -*-

"""
SQLite manager for persisting heatmap evaluations (minimal schema).

Table (exactly as requested)
---------------------------
CREATE TABLE IF NOT EXISTS heatmap_runs (
      unique_id                 INTEGER PRIMARY KEY AUTOINCREMENT
    , origin_name               TEXT        NOT NULL
    , origin_lat                REAL        NOT NULL
    , origin_lon                REAL        NOT NULL
    , destiny_name              TEXT        NOT NULL
    , destiny_lat               REAL        NOT NULL
    , destiny_lon               REAL        NOT NULL
    , cargo_weight_ton          REAL        NOT NULL

    , road_only_distance_km     REAL

    , cab_po_name               TEXT
    , cab_pd_name               TEXT

    , cab_road_o_to_po_km       REAL
    , cab_road_pd_to_d_km       REAL

    , is_hgv                    BOOL
    , insertion_timestamp       TIMESTAMP   NOT NULL DEFAULT (datetime('now'))
);

Uniqueness & policy
-------------------
• Uniqueness enforced via a UNIQUE INDEX on (origin_name, cargo_weight_ton, destiny_name)
  so ON CONFLICT upsert works while keeping `unique_id` as rowid PK.
• No other columns are stored; everything else is derivable from these inputs/distances.

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
# Connection helpers
# ────────────────────────────────────────────────────────────────────────────────

def _ensure_parent_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _configure_pragmas(conn: sqlite3.Connection) -> None:
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
# DDL — create table & indexes (ONLY the requested columns)
# ────────────────────────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
      unique_id                 INTEGER PRIMARY KEY AUTOINCREMENT
    , origin_name               TEXT        NOT NULL
    , origin_lat                REAL        NOT NULL
    , origin_lon                REAL        NOT NULL
    , destiny_name              TEXT        NOT NULL
    , destiny_lat               REAL        NOT NULL
    , destiny_lon               REAL        NOT NULL
    , cargo_weight_ton          REAL        NOT NULL

    , road_only_distance_km     REAL

    , cab_po_name               TEXT
    , cab_pd_name               TEXT

    , cab_road_o_to_po_km       REAL
    , cab_road_pd_to_d_km       REAL

    , is_hgv                    BOOL
    , insertion_timestamp       TIMESTAMP   NOT NULL DEFAULT (datetime('now'))
);
""".strip()

# Unique index for upsert semantics (no extra columns are added)
_CREATE_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_{table}_key
    ON {table} (origin_name, cargo_weight_ton, destiny_name);
""".strip()

# Optional helper index
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

def _bool_to_int(v: Optional[bool]) -> Optional[int]:
    if v is None:
        return None
    return 1 if bool(v) else 0


def upsert_run(
    conn: sqlite3.Connection
    , *
    , origin_name: str
    , origin_lat: float
    , origin_lon: float
    , destiny_name: str
    , destiny_lat: float
    , destiny_lon: float
    , cargo_weight_ton: float
    , road_only_distance_km: Optional[float] = None
    , cab_po_name: Optional[str] = None
    , cab_pd_name: Optional[str] = None
    , cab_road_o_to_po_km: Optional[float] = None
    , cab_road_pd_to_d_km: Optional[float] = None
    , is_hgv: Optional[bool] = None
    , table_name: str = DEFAULT_TABLE
) -> None:
    """
    Insert or update a row keyed by (origin_name, cargo_weight_ton, destiny_name).
    Leaves insertion_timestamp unchanged on updates.
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
        , cargo_weight_ton
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(origin_name, cargo_weight_ton, destiny_name) DO UPDATE SET
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
        , float(origin_lat)
        , float(origin_lon)
        , destiny_name
        , float(destiny_lat)
        , float(destiny_lon)
        , float(cargo_weight_ton)
        , (None if road_only_distance_km is None else float(road_only_distance_km))
        , cab_po_name
        , cab_pd_name
        , (None if cab_road_o_to_po_km is None else float(cab_road_o_to_po_km))
        , (None if cab_road_pd_to_d_km is None else float(cab_road_pd_to_d_km))
        , _bool_to_int(is_hgv)
    )
    conn.execute(sql, params)


def insert_if_absent(
    conn: sqlite3.Connection
    , *
    , origin_name: str
    , origin_lat: float
    , origin_lon: float
    , destiny_name: str
    , destiny_lat: float
    , destiny_lon: float
    , cargo_weight_ton: float
    , road_only_distance_km: Optional[float] = None
    , cab_po_name: Optional[str] = None
    , cab_pd_name: Optional[str] = None
    , cab_road_o_to_po_km: Optional[float] = None
    , cab_road_pd_to_d_km: Optional[float] = None
    , is_hgv: Optional[bool] = None
    , table_name: str = DEFAULT_TABLE
) -> bool:
    """
    Insert only; ignore if the (origin_name, cargo_weight_ton, destiny_name) exists.
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
        , cargo_weight_ton
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """.strip()

    cur = conn.execute(sql, (
          origin_name
        , float(origin_lat)
        , float(origin_lon)
        , destiny_name
        , float(destiny_lat)
        , float(destiny_lon)
        , float(cargo_weight_ton)
        , (None if road_only_distance_km is None else float(road_only_distance_km))
        , cab_po_name
        , cab_pd_name
        , (None if cab_road_o_to_po_km is None else float(cab_road_o_to_po_km))
        , (None if cab_road_pd_to_d_km is None else float(cab_road_pd_to_d_km))
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
    Bulk upsert rows. Each row dict must provide the same keys accepted by upsert_run.
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
        , cargo_weight_ton
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(origin_name, cargo_weight_ton, destiny_name) DO UPDATE SET
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

    def _row_to_params(r: Mapping[str, Any]) -> Tuple[Any, ...]:
        return (
              r["origin_name"]
            , float(r["origin_lat"])
            , float(r["origin_lon"])
            , r["destiny_name"]
            , float(r["destiny_lat"])
            , float(r["destiny_lon"])
            , float(r["cargo_weight_ton"])
            , (None if r.get("road_only_distance_km") is None else float(r["road_only_distance_km"]))
            , r.get("cab_po_name")
            , r.get("cab_pd_name")
            , (None if r.get("cab_road_o_to_po_km") is None else float(r["cab_road_o_to_po_km"]))
            , (None if r.get("cab_road_pd_to_d_km") is None else float(r["cab_road_pd_to_d_km"]))
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
    , keys: Sequence[Tuple[str, float, str]]
    , rows: Iterable[Mapping[str, Any]]
    , table_name: str = DEFAULT_TABLE
) -> int:
    """
    Overwrite semantics for a subset of composite keys:
        1) delete keys provided
        2) bulk-upsert replacement rows
    keys = sequence of (origin_name, cargo_weight_ton, destiny_name)
    """
    ensure_main_table(conn, table_name=table_name)

    del_sql = f"""
    DELETE FROM {table_name}
    WHERE origin_name = ?
      AND cargo_weight_ton = ?
      AND destiny_name = ?;
    """.strip()

    if keys:
        conn.executemany(del_sql, [
              (k[0], float(k[1]), k[2])
            for k in keys
        ])

    return bulk_upsert_runs(conn, rows=rows, table_name=table_name)


def get_run(
    conn: sqlite3.Connection
    , *
    , origin_name: str
    , cargo_weight_ton: float
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
        , cargo_weight_ton
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
        , insertion_timestamp
    FROM {table_name}
    WHERE origin_name = ?
      AND cargo_weight_ton = ?
      AND destiny_name = ?;
    """.strip()

    row = conn.execute(sql, (
          origin_name
        , float(cargo_weight_ton)
        , destiny_name
    )).fetchone()

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
        , cargo_weight_ton_v
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
        , "origin_lat": float(origin_lat_v)
        , "origin_lon": float(origin_lon_v)
        , "destiny_name": destiny_name_v
        , "destiny_lat": float(destiny_lat_v)
        , "destiny_lon": float(destiny_lon_v)
        , "cargo_weight_ton": float(cargo_weight_ton_v)
        , "road_only_distance_km": (None if road_only_distance_km_v is None else float(road_only_distance_km_v))
        , "cab_po_name": cab_po_name_v
        , "cab_pd_name": cab_pd_name_v
        , "cab_road_o_to_po_km": (None if cab_road_o_to_po_km_v is None else float(cab_road_o_to_po_km_v))
        , "cab_road_pd_to_d_km": (None if cab_road_pd_to_d_km_v is None else float(cab_road_pd_to_d_km_v))
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

    clauses = []
    params  = []

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
        , cargo_weight_ton
        , road_only_distance_km
        , cab_po_name
        , cab_pd_name
        , cab_road_o_to_po_km
        , cab_road_pd_to_d_km
        , is_hgv
        , insertion_timestamp
    FROM {table_name}
    {where}
    ORDER BY origin_name, destiny_name, cargo_weight_ton
    {lim};
    """.strip()

    out = []
    for row in conn.execute(sql, tuple(params)).fetchall():
        out.append({
              "unique_id": row[0]
            , "origin_name": row[1]
            , "origin_lat": float(row[2])
            , "origin_lon": float(row[3])
            , "destiny_name": row[4]
            , "destiny_lat": float(row[5])
            , "destiny_lon": float(row[6])
            , "cargo_weight_ton": float(row[7])
            , "road_only_distance_km": (None if row[8]  is None else float(row[8]))
            , "cab_po_name": row[9]
            , "cab_pd_name": row[10]
            , "cab_road_o_to_po_km": (None if row[11] is None else float(row[11]))
            , "cab_road_pd_to_d_km": (None if row[12] is None else float(row[12]))
            , "is_hgv": (None if row[13] is None else bool(row[13]))
            , "insertion_timestamp": row[14]
        })
    return out


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
            , cargo_weight_ton=50.0
            , road_only_distance_km=612.3
            , cab_po_name="Santos"
            , cab_pd_name="Itajaí"
            , cab_road_o_to_po_km=82.0
            , cab_road_pd_to_d_km=76.0
            , is_hgv=True
        )
        log.info("Rows (limit 3): %s", list_runs(_conn, limit=3))
