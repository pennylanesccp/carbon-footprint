#!/usr/bin/env python3
# scripts/routes_generator.py
# -*- coding: utf-8 -*-

"""
Precompute routing building blocks (road-only O→D, nearest ports road legs)
and persist in SQLite.

Caching logic:

  1) First, check if a row exists for the *raw* CLI inputs
     (origin = args.origin, destiny = args.destiny).
     If found and overwrite=False → skip everything.

  2) If not found, geocode origin/destiny.

  3) Then check again using the *resolved* labels
     (origin_name, destiny_name) that are actually stored in the DB.
     If found and overwrite=False → skip routing (no ORS directions).

  4) Otherwise, call ORS directions and upsert the row using the
     resolved names as keys.

Null-geocode behaviour:

  • If the origin is resolved but the destiny is not, persist a row with:
      - origin_* from the resolved origin
      - destiny_name = raw input (unresolved)
      - destiny_lat/lon = NULL
      - all distances = NULL

  • If the origin also cannot be resolved, skip DB persistence (origin_lat
    is NOT NULL in the main table).
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────────────────────
# Path bootstrap (must be first)
# ────────────────────────────────────────────────────────────────────────────────
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]  # repo root (one level above /scripts)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ────────────────────────────────────────────────────────────────────────────────
# Standard libs
# ────────────────────────────────────────────────────────────────────────────────
import argparse
import json
import logging
from typing import Any, Optional, Tuple

# ────────────────────────────────────────────────────────────────────────────────
# Project imports
# ────────────────────────────────────────────────────────────────────────────────
from modules.functions._logging import init_logging
from modules.app.evaluator import DataPaths

# DB utils
from modules.functions.database_manager import (
      db_session
    , ensure_main_table
    , get_run
    , upsert_run
    , delete_key
    , DEFAULT_DB_PATH
    , DEFAULT_TABLE
)

# ORS + geocoding
from modules.road.ors_common import ORSConfig, RateLimited, NoRoute
from modules.road.ors_client import ORSClient
from modules.road.addressing import resolve_point_null_safe as geo_resolve

# Cabotage helpers (ports + nearest-port search)
from modules.cabotage.ports_index import load_ports
from modules.cabotage.ports_nearest import find_nearest_port

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# CLI parser
# ────────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Precompute routing building blocks (road-only O→D, nearest ports road legs) "
            "and persist in SQLite."
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

    # Routing knobs (profile affects is_hgv and short road legs)
    parser.add_argument(
          "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS routing profile. Default: driving-hgv"
    )

    # Boolean args with fallback for Python <3.9
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
            , help="If True, delete existing row and recompute."
        )
    except Exception:
        # Python <3.9 compatibility
        parser.add_argument(
              "--fallback-to_car"
            , dest="fallback_to_car"
            , action="store_true"
            , default=True
        )
        parser.add_argument(
              "--no-fallback-to_car"
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

    # Data paths (sea matrix / ports)
    dp = DataPaths()
    parser.add_argument(
          "--ports-json"
        , type=Path
        , default=dp.ports_json
        , help="Path to ports_br.json."
    )
    parser.add_argument(
          "--sea-matrix"
        , type=Path
        , default=dp.sea_matrix_json
        , help="(Unused here) Path to sea_matrix.json."
    )
    parser.add_argument(
          "--hotel-json"
        , type=Path
        , default=dp.hotel_json
        , help="(Unused here) Path to hotel.json."
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
        , help=f"Target table. Default: {DEFAULT_TABLE}"
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
# ORS helpers
# ────────────────────────────────────────────────────────────────────────────────

def _route_distance_km(
      ors: ORSClient
    , origin: Any
    , destination: Any
    , primary_profile: str
    , fallback_to_car: bool
) -> Tuple[Optional[str], Optional[float]]:
    """
    Call ORS /v2/directions for origin→destination.

    Tries:
      1) primary_profile
      2) driving-car (if fallback_to_car and primary_profile != driving-car)

    Behaviour:
      • On success: returns (profile_used, distance_km).
      • On RateLimited: re-raises → caller should stop bulk run.
      • On NoRoute/other failures: logs and returns (last_profile_tried, None).
    """
    profiles: list[str] = [primary_profile]
    if fallback_to_car and primary_profile != "driving-car":
        profiles.append("driving-car")

    last_exc: Optional[Exception] = None

    for prof in profiles:
        try:
            res = ors.route_road(origin, destination, profile=prof)
            dist_m = res.get("distance_m")
            km = None if dist_m is None else float(dist_m) / 1000.0

            log.info(
                  "ROUTE origin→dest using %s: distance=%s km"
                , prof
                , "NULL" if km is None else f"{km:.3f}"
            )
            return prof, km

        except RateLimited:
            # Bubble up so bulk runner can break the loop cleanly
            raise

        except NoRoute as exc:
            log.warning(
                  "NoRoute for profile=%s origin=%r destination=%r: %s "
                  "→ storing NULL distance for this leg."
                , prof
                , origin
                , destination
                , exc
            )
            last_exc = exc

        except Exception as exc:      # noqa: BLE001
            log.error(
                  "Route failed for profile=%s origin=%r destination=%r: %s "
                  "→ storing NULL distance for this leg."
                , prof
                , origin
                , destination
                , exc
            )
            last_exc = exc

    if last_exc is not None:
        log.warning(
              "All route attempts failed for origin=%r destination=%r (profiles_tried=%r). "
              "Distance will be NULL."
            , origin
            , destination
            , profiles
        )

    # Return last profile tried (or None) and NULL distance
    return (profiles[-1] if profiles else None), None


def _port_anchor_point(
      port_info: dict[str, Any]
    , *
    , suffix: str = "gate"
) -> dict[str, Any]:
    """
    Build a (lat, lon, label) dict for routing to/from a port.

    If a gate is present, its coordinates are used; otherwise the port centroid.
    """
    gate = port_info.get("gate")
    if gate:
        return {
              "lat": float(gate["lat"])
            , "lon": float(gate["lon"])
            , "label": f"{port_info.get('name', 'port')} {suffix}".strip()
        }

    return {
          "lat": float(port_info["lat"])
        , "lon": float(port_info["lon"])
        , "label": str(port_info.get("name", "port"))
    }


# ────────────────────────────────────────────────────────────────────────────────
# NULL-geocode handling
# ────────────────────────────────────────────────────────────────────────────────

def _persist_null_geocode_run(
      args: argparse.Namespace
    , origin_raw: str
    , destiny_raw: str
    , origin_pt: Optional[dict[str, Any]]
    , destiny_pt: Optional[dict[str, Any]]
) -> dict[str, Any]:
    """
    Persist a run where at least one side could not be fully geocoded.

    Rules:
      - If the ORIGIN is resolved, store its coordinates and label
        (NOT NULL constraint on origin_lat/origin_lon is respected).
      - If the DESTINY is not resolved, use the raw input text as
        destiny_name and keep destiny_lat/destiny_lon as NULL.
      - If the ORIGIN is also not resolved, skip DB persistence (only
        return the JSON payload).
    """
    log.warning(
          "Origin or destiny could not be fully geocoded; handling as partial NULL run."
          " origin_raw=%r destiny_raw=%r"
        , origin_raw
        , destiny_raw
    )

    # Names: prefer resolved labels if present, otherwise raw inputs
    origin_name = (
        str(origin_pt.get("label"))
        if origin_pt and origin_pt.get("label") is not None
        else str(origin_raw)
    )
    destiny_name = (
        str(destiny_pt.get("label"))
        if destiny_pt and destiny_pt.get("label") is not None
        else str(destiny_raw)
    )

    # Coordinates (may be None when not resolved)
    origin_lat = (
        float(origin_pt["lat"])
        if origin_pt and origin_pt.get("lat") is not None
        else None
    )
    origin_lon = (
        float(origin_pt["lon"])
        if origin_pt and origin_pt.get("lon") is not None
        else None
    )
    destiny_lat = (
        float(destiny_pt["lat"])
        if destiny_pt and destiny_pt.get("lat") is not None
        else None
    )
    destiny_lon = (
        float(destiny_pt["lon"])
        if destiny_pt and destiny_pt.get("lon") is not None
        else None
    )

    # If origin coords are missing, we cannot satisfy NOT NULL(origin_lat).
    # In this case, only log + return payload (no DB write).
    if origin_lat is None or origin_lon is None:
        log.info(
              "Skipping DB persistence for NULL-geocode run because origin coords"
              " are missing and origin_lat/origin_lon are NOT NULL."
              " origin_raw=%r destiny_raw=%r"
            , origin_name
            , destiny_raw
        )
        payload = {
              "origin": origin_name
            , "destiny": destiny_raw
            , "road_only_distance_km": None
            , "cabotage": {
                  "port_origin": None
                , "port_destiny": None
                , "road_o_to_po_km": None
                , "road_pd_to_d_km": None
            }
            , "profile_used": None
        }
        log.info(
              "Routes generator finished (NULL geocode, no DB write) for (%s → %s)."
            , origin_name
            , destiny_name
        )
        return payload

    # At this point origin is valid; destiny may or may not be.
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)
        upsert_run(
              conn
            , origin_name=origin_name
            , origin_lat=origin_lat
            , origin_lon=origin_lon
            , destiny_name=destiny_raw
            , destiny_lat=destiny_lat
            , destiny_lon=destiny_lon
            , road_only_distance_km=None
            , cab_po_name=None
            , cab_pd_name=None
            , cab_road_o_to_po_km=None
            , cab_road_pd_to_d_km=None
            , is_hgv=None
            , table_name=args.table
        )

    payload = {
          "origin": origin_pt
        , "destiny": destiny_raw
        , "road_only_distance_km": None
        , "cabotage": {
              "port_origin": None
            , "port_destiny": None
            , "road_o_to_po_km": None
            , "road_pd_to_d_km": None
        }
        , "profile_used": None
    }

    log.info(
          "Routes generator finished (NULL/partial geocode) for (%s → %s)."
        , origin_name
        , destiny_raw
    )
    return payload


# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────

def main(argv: Optional[list[str]] = None) -> int:
    # ------------------------------------------------------------------
    # Parse CLI args + configure logging
    # ------------------------------------------------------------------
    args = _build_parser().parse_args(argv)

    init_logging(level=args.log_level, force=True, write_output=False)
    log.info(
          "Routes generator starting for origin=%r destiny=%r [profile=%s, overwrite=%s]"
        , args.origin
        , args.destiny
        , args.ors_profile
        , args.overwrite
    )

    # ------------------------------------------------------------------
    # 0) Early DB gate on RAW input strings
    # ------------------------------------------------------------------
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)

        exists_input = get_run(
              conn
            , origin_name=args.origin
            , destiny_name=args.destiny
            , table_name=args.table
        )

        if exists_input and not args.overwrite:
            log.info(
                  "Cache hit (raw input names) for (%s → %s); skipping geocoding and routing."
                , args.origin
                , args.destiny
            )
            return 0

        if exists_input and args.overwrite:
            delete_key(
                  conn
                , origin_name=args.origin
                , destiny_name=args.destiny
                , table_name=args.table
            )
            log.info(
                  "Overwrite=True ⇒ deleted existing row for raw pair (%s → %s) before recompute."
                , args.origin
                , args.destiny
            )

    # Local ORS client for geocoding + routing
    ors = ORSClient(cfg=ORSConfig())

    # ------------------------------------------------------------------
    # 1) Geocode origin/destiny once (later reused for all legs)
    #    • resolve_point_null_safe may return None → handle gracefully.
    # ------------------------------------------------------------------
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
        payload = _persist_null_geocode_run(
              args=args
            , origin_raw=args.origin
            , destiny_raw=args.destiny
            , origin_pt=origin_pt
            , destiny_pt=destiny_pt
        )

        if args.pretty:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0

    # At this point geocoding is OK
    origin_name = str(origin_pt.get("label") or args.origin)
    destiny_name = str(destiny_pt.get("label") or args.destiny)

    # ------------------------------------------------------------------
    # 2) Second DB gate on RESOLVED names (the canonical key)
    # ------------------------------------------------------------------
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)

        exists_resolved = get_run(
              conn
            , origin_name=origin_name
            , destiny_name=destiny_name
            , table_name=args.table
        )

        if exists_resolved and not args.overwrite:
            log.info(
                  "Cache hit (resolved names) for (%s → %s); "
                  "skipping routing API calls."
                , origin_name
                , destiny_name
            )
            return 0

        if exists_resolved and args.overwrite:
            delete_key(
                  conn
                , origin_name=origin_name
                , destiny_name=destiny_name
                , table_name=args.table
            )
            log.info(
                  "Overwrite=True ⇒ deleted existing row for resolved pair (%s → %s) before recompute."
                , origin_name
                , destiny_name
            )

    # ------------------------------------------------------------------
    # 3) Main road route O→D
    #    • If this fails (road_only_km is None), DO NOT compute O→PO / PD→D.
    # ------------------------------------------------------------------
    primary_profile = args.ors_profile

    profile_used_road, road_only_km = _route_distance_km(
          ors
        , origin_pt
        , destiny_pt
        , primary_profile=primary_profile
        , fallback_to_car=args.fallback_to_car
    )

    # Defaults for cabotage (may stay None if road-only fails)
    o_port: Optional[dict[str, Any]] = None
    d_port: Optional[dict[str, Any]] = None
    po_name: Optional[str] = None
    pd_name: Optional[str] = None
    road_o_to_po_km: Optional[float] = None
    road_pd_to_d_km: Optional[float] = None

    # Determine is_hgv only if main road distance is known
    if road_only_km is None:
        is_hgv: Optional[bool] = None
    else:
        if profile_used_road == "driving-hgv":
            is_hgv = True
        elif profile_used_road == "driving-car":
            is_hgv = False
        else:
            is_hgv = None

    # ------------------------------------------------------------------
    # 4) Only if road-only O→D exists, compute nearest ports and port legs.
    # ------------------------------------------------------------------
    if road_only_km is not None:
        ports = load_ports(path=str(args.ports_json))

        o_port = find_nearest_port(origin_pt["lat"], origin_pt["lon"], ports)
        d_port = find_nearest_port(destiny_pt["lat"], destiny_pt["lon"], ports)

        po_name = str(o_port["name"])
        pd_name = str(d_port["name"])

        po_anchor = _port_anchor_point(o_port, suffix="gate")
        pd_anchor = _port_anchor_point(d_port, suffix="gate")

        _profile_o_po, road_o_to_po_km = _route_distance_km(
              ors
            , origin_pt
            , po_anchor
            , primary_profile=primary_profile
            , fallback_to_car=args.fallback_to_car
        )

        _profile_pd_d, road_pd_to_d_km = _route_distance_km(
              ors
            , pd_anchor
            , destiny_pt
            , primary_profile=primary_profile
            , fallback_to_car=args.fallback_to_car
        )

        log.info(
              "Extracted geo and distances: origin=%r (%.6f,%.6f) → destiny=%r (%.6f,%.6f) "
              "road_only_km=%s cab_po=%r cab_pd=%r o→po_km=%s pd→d_km=%s"
            , origin_name
            , float(origin_pt["lat"])
            , float(origin_pt["lon"])
            , destiny_name
            , float(destiny_pt["lat"])
            , float(destiny_pt["lon"])
            , road_only_km
            , po_name
            , pd_name
            , road_o_to_po_km
            , road_pd_to_d_km
        )
    else:
        log.warning(
              "No road route origin→destiny for (%s → %s); skipping O→PO and PD→D legs."
            , origin_name
            , destiny_name
        )

    # ------------------------------------------------------------------
    # 5) Persist in SQLite (single upsert row)
    # ------------------------------------------------------------------
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)
        upsert_run(
              conn
            , origin_name=origin_name
            , origin_lat=float(origin_pt["lat"])
            , origin_lon=float(origin_pt["lon"])
            , destiny_name=destiny_name
            , destiny_lat=float(destiny_pt["lat"])
            , destiny_lon=float(destiny_pt["lon"])
            , road_only_distance_km=road_only_km
            , cab_po_name=po_name
            , cab_pd_name=pd_name
            , cab_road_o_to_po_km=road_o_to_po_km
            , cab_road_pd_to_d_km=road_pd_to_d_km
            , is_hgv=is_hgv
            , table_name=args.table
        )

    # ------------------------------------------------------------------
    # 6) Optional JSON echo (handy for CI logs / quick checks)
    # ------------------------------------------------------------------
    payload = {
          "origin": origin_pt
        , "destiny": destiny_pt
        , "road_only_distance_km": road_only_km
        , "cabotage": {
              "port_origin": o_port
            , "port_destiny": d_port
            , "road_o_to_po_km": road_o_to_po_km
            , "road_pd_to_d_km": road_pd_to_d_km
        }
        , "profile_used": profile_used_road if road_only_km is not None else None
    }

    if args.pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    log.info(
          "Routes generator finished for (%s → %s)."
        , origin_name
        , destiny_name
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
