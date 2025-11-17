#!/usr/bin/env python3
# app/multimodal_route_builder.py
# -*- coding: utf-8 -*-

"""
Multimodal legs builder (road + ports + sea)
===========================================

Purpose
-------
Given a single (origin, destiny) pair, this script:

  • Geocodes origin and destiny using ORS.
  • Finds the nearest port to each point (gate-aware) using:
        - modules.ports.ports_index.load_ports
        - modules.ports.ports_nearest.find_nearest_port
  • Precomputes and persists in SQLite the ROAD legs:
        - origin_name       → destiny_name        (road-only baseline)
        - origin_name       → origin_port_name    (truck to origin port)
        - destiny_port_name → destiny_name        (truck from destination port)
    via modules.infra.database_manager.upsert_run.

  • Computes the SEA distance between origin_port and destiny_port using:
        - modules.cabotage.sea_matrix.SeaMatrix

  • Emits a JSON summary for the O/D pair on stdout.

DB caching semantics
--------------------
For each road leg (origin_label, destiny_label):

  1) Before calling ORS, the SQLite cache is checked:
        - if ANY row exists for (origin_label, destiny_label), regardless of
          is_hgv, the API call is skipped.

  2) If geocoding fails or ORS cannot return a route, a row is still upserted
     with:
        - distance_km = NULL
        - is_hgv      = NULL
     to avoid reprocessing the same pair endlessly.

  3) If the error is due to “Quota Exceeded” / rate limit (RateLimited or a
     message containing 'quota' and 'exceeded'), the script exits immediately.

  4) If --overwrite is passed, the cache step is ignored and the leg is
     always recomputed and upserted.

CLI usage
---------
python -m app.multimodal_route_builder ^
    --origin  "Avenida Professor Luciano Gualberto, São Paulo" ^
    --destiny "Fortaleza, CE" ^
    --overwrite ^
    --pretty

Options:
  --ors-profile          driving-hgv | driving-car (default: driving-hgv)
  --no-fallback-to-car   disable automatic fallback to driving-car
  --db-path              SQLite DB path (default: modules.infra.database_manager.DEFAULT_DB_PATH)
  --table                routes table name (default: DEFAULT_TABLE)
  --ports-json           ports JSON path (default: data/cabotage_data/ports_br.json)
  --sea-matrix-json      sea matrix JSON path (default: data/cabotage_data/sea_matrix.json)
  --overwrite / --no-overwrite
                         Recompute legs even if cached (default: False).
  --log-level            DEBUG | INFO | WARNING | ERROR (default: INFO)
  --pretty               pretty-print JSON to stdout
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────────────────────
# Path bootstrap (app → modules)
# ────────────────────────────────────────────────────────────────────────────────
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]  # repo root (one level above /app)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ────────────────────────────────────────────────────────────────────────────────
# Standard library
# ────────────────────────────────────────────────────────────────────────────────
import argparse
import json
import logging
from typing import Any, Dict, Optional, Tuple

# ────────────────────────────────────────────────────────────────────────────────
# Project imports
# ────────────────────────────────────────────────────────────────────────────────
from modules.infra.logging import init_logging
from modules.infra.database_manager import (
      db_session
    , ensure_main_table
    , list_runs
    , upsert_run
    , delete_key            # ← add this
    , DEFAULT_DB_PATH
    , DEFAULT_TABLE
)

from modules.road.ors_common import ORSConfig, RateLimited, NoRoute
from modules.road.ors_client import ORSClient

from modules.addressing.resolver import (
      resolve_point_null_safe as geo_resolve
)

from modules.ports.ports_index import load_ports
from modules.ports.ports_nearest import find_nearest_port
from modules.cabotage.sea_matrix import SeaMatrix

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# Defaults for data files
# ────────────────────────────────────────────────────────────────────────────────
DEFAULT_PORTS_JSON      = ROOT / "data" / "processed" / "cabotage_data" / "ports_br.json"
DEFAULT_SEA_MATRIX_JSON = ROOT / "data" / "processed" / "cabotage_data" / "sea_matrix.json"


# ────────────────────────────────────────────────────────────────────────────────
# ORS helpers
# ────────────────────────────────────────────────────────────────────────────────
def _route_distance_km(
      ors: ORSClient
    , origin_name: str
    , destiny_name: str
    , primary_profile: str
    , fallback_to_car: bool
) -> Tuple[Optional[str], Optional[float]]:
    """
    Call ORS /v2/directions for origin→destiny **using string labels**.

    Tries:
      1) primary_profile
      2) driving-car (if fallback_to_car and primary_profile != 'driving-car')

    Behaviour:
      • On success: returns (profile_used, distance_km).
      • On RateLimited: re-raises → caller must abort orchestration.
      • On NoRoute / other failures: logs and returns (last_profile_tried, None).
    """
    profiles: list[str] = [primary_profile]
    if fallback_to_car and primary_profile != "driving-car":
        profiles.append("driving-car")

    last_exc: Optional[Exception] = None

    for prof in profiles:
        try:
            # IMPORTANT: pass strings, not GeoPoint
            res = ors.route_road(origin_name, destiny_name, profile=prof)
            dist_m = res.get("distance_m")
            km = None if dist_m is None else float(dist_m) / 1000.0

            log.info(
                  "ROUTE origin→dest using %s: distance=%s km"
                , prof
                , "NULL" if km is None else f"{km:.3f}"
            )
            return prof, km

        except RateLimited:
            # Bubble up so the top-level can treat as "quota exceeded" and abort
            raise

        except NoRoute as exc:
            log.warning(
                  "NoRoute for profile=%s origin=%r destination=%r: %s "
                  "→ storing NULL distance for this leg."
                , prof
                , origin_name
                , destiny_name
                , exc
            )
            last_exc = exc

        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if "quota" in msg and "exceeded" in msg:
                # Treat explicit quota messages as fatal as well
                log.error(
                      "Quota exceeded while routing (profile=%s origin=%r destination=%r): %s"
                    , prof
                    , origin_name
                    , destiny_name
                    , exc
                )
                raise RateLimited(exc)  # re-wrap so caller handles uniformly

            log.error(
                  "Route failed for profile=%s origin=%r destination=%r: %s "
                  "→ storing NULL distance for this leg."
                , prof
                , origin_name
                , destiny_name
                , exc
            )
            last_exc = exc

    if last_exc is not None:
        log.warning(
              "All route attempts failed for origin=%r destination=%r (profiles_tried=%r). "
              "Distance will be NULL."
            , origin_name
            , destiny_name
            , profiles
        )

    # Return last profile tried (or None) and NULL distance
    return (profiles[-1] if profiles else None), None


# ────────────────────────────────────────────────────────────────────────────────
# DB helper for a single leg
# ────────────────────────────────────────────────────────────────────────────────
def _ensure_road_leg(
      ors: ORSClient
    , *
    , origin_name: str
    , origin_lat: float
    , origin_lon: float
    , destiny_name: str
    , destiny_lat: float
    , destiny_lon: float
    , db_path: Path | str
    , table_name: str
    , primary_profile: str
    , fallback_to_car: bool
    , overwrite: bool
) -> Dict[str, Any]:
    """
    Ensure a single road leg is present in the SQLite cache.

    1) If overwrite is False:
         - Check cache for any row (origin_name, destiny_name, any is_hgv).
         - If present, return its info and skip ORS.

    2) If overwrite is True:
         - Delete *all* rows for (origin_name, destiny_name), regardless of is_hgv.
         - Recompute via ORS and upsert a single fresh row.

    Returns
    -------
    Dict[str, Any]
        {
          "origin_name", "destiny_name",
          "distance_km", "is_hgv", "profile_used",
          "cached": bool
        }
    """
    # First, handle cache / deletion
    with db_session(db_path=db_path) as conn:
        ensure_main_table(conn, table_name=table_name)

        if not overwrite:
            existing = list_runs(
                  conn
                , origin=origin_name
                , destiny=destiny_name
                , is_hgv=None   # do not filter by profile in cache check
                , table_name=table_name
                , limit=1
            )
            if existing:
                row = existing[0]
                log.info(
                      "Cache hit for leg (%s → %s); distance_km=%s is_hgv=%r"
                    , origin_name
                    , destiny_name
                    , row.get("distance_km")
                    , row.get("is_hgv")
                )
                return {
                      "origin_name": row["origin"]
                    , "destiny_name": row["destiny"]
                    , "distance_km": row.get("distance_km")
                    , "is_hgv": row.get("is_hgv")
                    , "profile_used": None
                    , "cached": True
                }
        else:
            # Hard overwrite semantics: drop any existing profiles for this leg
            log.info(
                  "overwrite=True ⇒ deleting existing rows for leg (%s → %s) before recompute."
                , origin_name
                , destiny_name
            )
            # is_hgv=None here means "all profiles for this pair"
            delete_key(
                  conn
                , origin=origin_name
                , destiny=destiny_name
                , is_hgv=None
                , table_name=table_name
            )

    # Not cached (or we deleted it) → call ORS using the labels
    try:
        profile_used, distance_km = _route_distance_km(
              ors=ors
            , origin_name=origin_name
            , destiny_name=destiny_name
            , primary_profile=primary_profile
            , fallback_to_car=fallback_to_car
        )
    except RateLimited as exc:
        log.error(
              "RateLimited / quota exceeded while computing leg (%s → %s). Aborting."
            , origin_name
            , destiny_name
        )
        raise SystemExit(1) from exc

    # Determine is_hgv
    if distance_km is None:
        is_hgv: Optional[bool] = None
    else:
        if profile_used == "driving-hgv":
            is_hgv = True
        elif profile_used == "driving-car":
            is_hgv = False
        else:
            is_hgv = None

    log.info(
          "Extracted leg: origin=%r (%.6f,%.6f) → destiny=%r (%.6f,%.6f) "
          "distance_km=%s is_hgv=%r profile=%s"
        , origin_name
        , float(origin_lat)
        , float(origin_lon)
        , destiny_name
        , float(destiny_lat)
        , float(destiny_lon)
        , "NULL" if distance_km is None else f"{distance_km:.3f}"
        , is_hgv
        , profile_used
    )

    # Persist
    with db_session(db_path=db_path) as conn:
        ensure_main_table(conn, table_name=table_name)
        upsert_run(
              conn
            , origin=origin_name
            , origin_lat=float(origin_lat)
            , origin_lon=float(origin_lon)
            , destiny=destiny_name
            , destiny_lat=float(destiny_lat)
            , destiny_lon=float(destiny_lon)
            , distance_km=distance_km
            , is_hgv=is_hgv
            , table_name=table_name
        )

    return {
          "origin_name": origin_name
        , "destiny_name": destiny_name
        , "distance_km": distance_km
        , "is_hgv": is_hgv
        , "profile_used": profile_used
        , "cached": False
    }


# ────────────────────────────────────────────────────────────────────────────────
# CLI parser
# ────────────────────────────────────────────────────────────────────────────────
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build multimodal legs (road-only + ports + sea) for a single O→D pair "
            "and persist road legs in SQLite."
        )
    )

    # Required spatial inputs
    parser.add_argument(
          "--origin"
        , required=True
        , help="Origin (address/city/CEP/'lat,lon')."
    )
    parser.add_argument(
          "--destiny"
        , required=True
        , help="Destiny (address/city/CEP/'lat,lon')."
    )

    # Routing knobs
    parser.add_argument(
          "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS routing profile. Default: driving-hgv"
    )

    try:
        from argparse import BooleanOptionalAction

        parser.add_argument(
              "--fallback-to-car"
            , default=True
            , action=BooleanOptionalAction
            , help="Retry with driving-car if primary fails. Default: True"
        )
        parser.add_argument(
              "--overwrite"
            , default=False
            , action=BooleanOptionalAction
            , help="Recompute legs even if a cached row exists. Default: False"
        )
    except Exception:
        parser.add_argument(
              "--fallback-to-car"
            , dest="fallback_to_car"
            , action="store_true"
            , default=True
        )
        parser.add_argument(
              "--no-fallback-to-car"
            , dest="fallback_to_car"
            , action="store_false"
        )
        parser.add_argument(
              "--overwrite"
            , dest="overwrite"
            , action="store_true"
            , default=False
        )
        parser.add_argument(
              "--no-overwrite"
            , dest="overwrite"
            , action="store_false"
        )

    # DB params
    parser.add_argument(
          "--db-path"
        , type=Path
        , default=DEFAULT_DB_PATH
        , help=f"SQLite path. Default: {DEFAULT_DB_PATH}"
    )
    parser.add_argument(
          "--table"
        , default=DEFAULT_TABLE
        , help=f"Target routes table. Default: {DEFAULT_TABLE}"
    )

    # Ports + sea matrix
    parser.add_argument(
          "--ports-json"
        , type=Path
        , default=DEFAULT_PORTS_JSON
        , help=f"Ports JSON path. Default: {DEFAULT_PORTS_JSON}"
    )
    parser.add_argument(
          "--sea-matrix-json"
        , type=Path
        , default=DEFAULT_SEA_MATRIX_JSON
        , help=f"Sea matrix JSON path. Default: {DEFAULT_SEA_MATRIX_JSON}"
    )

    # Output + logging
    parser.add_argument(
          "--pretty"
        , action="store_true"
        , help="Pretty-print JSON."
    )
    parser.add_argument(
          "--log-level"
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )

    return parser


# ────────────────────────────────────────────────────────────────────────────────
# Main orchestration
# ────────────────────────────────────────────────────────────────────────────────
def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)

    init_logging(level=args.log_level, force=True, write_output=False)
    log.info(
          "Multimodal builder starting for origin=%r destiny=%r "
          "[profile=%s, overwrite=%s]"
        , args.origin
        , args.destiny
        , args.ors_profile
        , args.overwrite
    )

    ors = ORSClient(cfg=ORSConfig())

    ports = load_ports(path=str(args.ports_json))
    sea_matrix = SeaMatrix.from_json_path(args.sea_matrix_json)

    # 1) Geocode origin/destiny
    origin_pt = geo_resolve(
          value=args.origin
        , ors=ors
        , log=log
    )
    destiny_pt = geo_resolve(
          value=args.destiny
        , ors=ors
        , log=log
    )

    if origin_pt is None or destiny_pt is None:
        log.warning(
              "At least one side could not be geocoded; inserting NULL road-only leg "
              "for raw inputs and skipping ports/sea. origin_raw=%r destiny_raw=%r"
            , args.origin
            , args.destiny
        )
        with db_session(db_path=args.db_path) as conn:
            ensure_main_table(conn, table_name=args.table)
            existing = list_runs(
                  conn
                , origin=args.origin
                , destiny=args.destiny
                , is_hgv=None
                , table_name=args.table
                , limit=1
            )
            if not existing or args.overwrite:
                upsert_run(
                      conn
                    , origin=args.origin
                    , origin_lat=None
                    , origin_lon=None
                    , destiny=args.destiny
                    , destiny_lat=None
                    , destiny_lon=None
                    , distance_km=None
                    , is_hgv=None
                    , table_name=args.table
                )
                log.info(
                    "Inserted/overwrote NULL leg for (%s → %s) due to geocode failure.",
                    args.origin, args.destiny
                )
            else:
                log.info(
                    "NULL leg for (%s → %s) already present; not inserting again.",
                    args.origin, args.destiny
                )

        payload = {
              "origin_raw": args.origin
            , "destiny_raw": args.destiny
            , "status": "geocode_failed"
        }
        if args.pretty:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0

    origin_name = str(origin_pt.label or args.origin)
    destiny_name = str(destiny_pt.label or args.destiny)
    origin_lat = float(origin_pt.lat)
    origin_lon = float(origin_pt.lon)
    destiny_lat = float(destiny_pt.lat)
    destiny_lon = float(destiny_pt.lon)

    # 2) Find nearest ports
    origin_port = find_nearest_port(origin_lat, origin_lon, ports)
    destiny_port = find_nearest_port(destiny_lat, destiny_lon, ports)

    def _port_anchor(p: Dict[str, Any]) -> Tuple[float, float]:
        gate = p.get("gate")
        if gate and isinstance(gate, dict):
            return float(gate["lat"]), float(gate["lon"])
        return float(p["lat"]), float(p["lon"])

    oport_lat, oport_lon = _port_anchor(origin_port)
    dport_lat, dport_lon = _port_anchor(destiny_port)

    # 3) Sea distance
    sea_km, sea_source = sea_matrix.km_with_source(
        {
            "name": origin_port["name"],
            "lat": origin_port["lat"],
            "lon": origin_port["lon"],
        },
        {
            "name": destiny_port["name"],
            "lat": destiny_port["lat"],
            "lon": destiny_port["lon"],
        },
    )

    # 4) ROAD legs
    road_only = _ensure_road_leg(
          ors
        , origin_name=origin_name
        , origin_lat=origin_lat
        , origin_lon=origin_lon
        , destiny_name=destiny_name
        , destiny_lat=destiny_lat
        , destiny_lon=destiny_lon
        , db_path=args.db_path
        , table_name=args.table
        , primary_profile=args.ors_profile
        , fallback_to_car=args.fallback_to_car
        , overwrite=args.overwrite
    )

    leg_origin_to_port = _ensure_road_leg(
          ors
        , origin_name=origin_name
        , origin_lat=origin_lat
        , origin_lon=origin_lon
        , destiny_name=origin_port["name"]
        , destiny_lat=oport_lat
        , destiny_lon=oport_lon
        , db_path=args.db_path
        , table_name=args.table
        , primary_profile=args.ors_profile
        , fallback_to_car=args.fallback_to_car
        , overwrite=args.overwrite
    )

    leg_port_to_destiny = _ensure_road_leg(
          ors
        , origin_name=destiny_port["name"]
        , origin_lat=dport_lat
        , origin_lon=dport_lon
        , destiny_name=destiny_name
        , destiny_lat=destiny_lat
        , destiny_lon=destiny_lon
        , db_path=args.db_path
        , table_name=args.table
        , primary_profile=args.ors_profile
        , fallback_to_car=args.fallback_to_car
        , overwrite=args.overwrite
    )

    # 5) Final payload
    payload: Dict[str, Any] = {
        "origin": {
              "input": args.origin
            , "label": origin_name
            , "lat": origin_lat
            , "lon": origin_lon
        },
        "destiny": {
              "input": args.destiny
            , "label": destiny_name
            , "lat": destiny_lat
            , "lon": destiny_lon
        },
        "ports": {
            "origin_port": origin_port,
            "destiny_port": destiny_port,
        },
        "legs": {
            "road_only": road_only,
            "origin_to_port": leg_origin_to_port,
            "port_to_destiny": leg_port_to_destiny,
        },
        "sea": {
            "distance_km": float(sea_km),
            "source": sea_source,
        },
        "status": "ok",
    }

    if args.pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    log.info(
          "Multimodal builder finished for (%s → %s)."
        , origin_name
        , destiny_name
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
