#!/usr/bin/env python3
# scripts/routes_generator.py
# -*- coding: utf-8 -*-

from __future__ import annotations

# --- path bootstrap (must be the first lines of the file) ---
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]  # repo root (one level above /scripts)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ------------------------------------------------------------

import argparse
import json
import logging
from typing import Any, Optional, Tuple

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
from modules.road.ors_common import ORSConfig
from modules.road.ors_client import ORSClient
from modules.addressing.resolver import resolve_point as geo_resolve

# Cabotage helpers (ports + nearest-port search)
from modules.cabotage.ports_index import load_ports
from modules.cabotage.ports_nearest import find_nearest_port

log = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Precompute routing building blocks (road-only O→D, nearest ports road legs) "
            "and persist in SQLite."
        )
    )
    p.add_argument("--origin", required=True, help="Origin (address/city/CEP/'lat,lon').")
    p.add_argument("--destiny", required=True, help="Destiny (address/city/CEP/'lat,lon').")

    # routing knobs (profile affects 'is_hgv' and short road legs)
    p.add_argument(
          "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS routing profile. Default: driving-hgv"
    )
    try:
        from argparse import BooleanOptionalAction

        p.add_argument(
              "--fallback-to-car"
            , default=True
            , action=BooleanOptionalAction
            , help="Retry with driving-car if primary fails. Default: True"
        )
        p.add_argument(
              "--overwrite"
            , default=False
            , action=BooleanOptionalAction
            , help="If True, delete existing row and recompute."
        )
    except Exception:
        # Python <3.9 compatibility
        p.add_argument("--fallback-to_car", dest="fallback_to_car", action="store_true", default=True)
        p.add_argument("--no-fallback-to_car", dest="fallback_to_car", action="store_false")
        p.add_argument("--overwrite", dest="overwrite", action="store_true", default=False)
        p.add_argument("--no-overwrite", dest="overwrite", action="store_false")

    # data paths (sea matrix / ports)
    dp = DataPaths()
    p.add_argument("--ports-json", type=Path, default=dp.ports_json, help="Path to ports_br.json.")
    p.add_argument("--sea-matrix", type=Path, default=dp.sea_matrix_json, help="(Unused here) Path to sea_matrix.json.")
    p.add_argument("--hotel-json", type=Path, default=dp.hotel_json, help="(Unused here) Path to hotel.json.")

    # DB params
    p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help=f"SQLite path. Default: {DEFAULT_DB_PATH}")
    p.add_argument("--table", default=DEFAULT_TABLE, help=f"Target table. Default: {DEFAULT_TABLE}")

    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON.")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _route_distance_km(
      ors: ORSClient
    , origin: Any
    , destination: Any
    , primary_profile: str
    , fallback_to_car: bool
) -> Tuple[str, Optional[float]]:
    """
    Call ORS /v2/directions once with *primary_profile* and, if requested,
    retry with driving-car on failure.

    Returns
    -------
    (profile_used, distance_km_or_None)
    """
    used_profile = primary_profile
    try:
        res = ors.route_road(origin, destination, profile=primary_profile)
    except Exception as e:
        if fallback_to_car and primary_profile != "driving-car":
            log.warning(
                  "Primary '%s' failed (%s). Falling back to 'driving-car'."
                , primary_profile
                , e
            )
            used_profile = "driving-car"
            res = ors.route_road(origin, destination, profile="driving-car")
        else:
            log.error(
                  "Route failed for profile=%s origin=%r destination=%r: %s"
                , primary_profile
                , origin
                , destination
                , e
            )
            raise

    dist_m = res.get("distance_m")
    km: Optional[float]
    km = None if dist_m is None else float(dist_m) / 1000.0
    return used_profile, km


def _port_anchor_point(port_info: dict[str, Any], *, suffix: str = "gate") -> dict[str, Any]:
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

def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)

    init_logging(level=args.log_level, force=True, write_output=False)
    log.info(
          "Routes generator starting for origin=%r destiny=%r [profile=%s, overwrite=%s]"
        , args.origin
        , args.destiny
        , args.ors_profile
        , args.overwrite
    )

    # local ORS client for geocoding + routing
    ors = ORSClient(cfg=ORSConfig())

    # ------------------------------------------------------------------
    # DB gate + overwrite policy (use raw input strings as cache key)
    # ------------------------------------------------------------------
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)

        exists = get_run(
              conn
            , origin_name=args.origin
            , destiny_name=args.destiny
            , table_name=args.table
        )

        if not args.overwrite and exists:
            log.debug("Cache hit (overwrite=False). Skipping API calls.")
            return 0

        if args.overwrite and exists:
            delete_key(
                  conn
                , origin_name=args.origin
                , destiny_name=args.destiny
                , table_name=args.table
            )
            log.debug("Overwrite=True ⇒ deleted existing row before recompute.")

    # ------------------------------------------------------------------
    # 1) Geocode origin/destiny once (later reused for all legs)
    # ------------------------------------------------------------------
    origin_pt = geo_resolve(args.origin, ors=ors)    # {'lat','lon','label'}
    destiny_pt = geo_resolve(args.destiny, ors=ors)  # {'lat','lon','label'}

    origin_name = str(origin_pt.get("label") or args.origin)
    destiny_name = str(destiny_pt.get("label") or args.destiny)

    # ------------------------------------------------------------------
    # 2) Load ports + find nearest to origin/destiny (haversine, gate-aware)
    # ------------------------------------------------------------------
    ports = load_ports(path=str(args.ports_json))

    o_port = find_nearest_port(origin_pt["lat"], origin_pt["lon"], ports)
    d_port = find_nearest_port(destiny_pt["lat"], destiny_pt["lon"], ports)

    po_name = str(o_port["name"])
    pd_name = str(d_port["name"])

    po_anchor = _port_anchor_point(o_port, suffix="gate")
    pd_anchor = _port_anchor_point(d_port, suffix="gate")

    # ------------------------------------------------------------------
    # 3) Road routes (direct O→D, O→Po, Pd→D) with fallback profile logic
    # ------------------------------------------------------------------
    primary_profile = args.ors_profile

    profile_used_road, road_only_km = _route_distance_km(
          ors
        , origin_pt
        , destiny_pt
        , primary_profile=primary_profile
        , fallback_to_car=args.fallback_to_car
    )

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

    # is_hgv: infer from the main road leg profile actually used
    if profile_used_road == "driving-hgv":
        is_hgv: Optional[bool] = True
    elif profile_used_road == "driving-car":
        is_hgv = False
    else:
        is_hgv = None

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

    # ------------------------------------------------------------------
    # 4) Persist in SQLite (single upsert row)
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
    # 5) Optional JSON echo (handy for CI logs / quick checks)
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
        , "profile_used": profile_used_road
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
