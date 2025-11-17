# modules/fuel/multimodal_fuel_service.py
# -*- coding: utf-8 -*-
"""
Multimodal fuel service (road-only vs cabotage)
===============================================

Purpose
-------
Given a single origin/destiny pair and a cargo mass (t), orchestrate:

  1) Route building (via the same machinery used by app.multimodal_route_builder):
       - geocode origin/destiny using ORS,
       - find nearest ports,
       - cache road legs in SQLite (origin→destiny, origin→origin_port, destiny_port→destiny),
       - compute sea distance between ports (for info only).

  2) Fuel estimation:
       - Road legs → call modules.fuel.road_fuel_service.get_road_fuel_profile
         to get km/L and diesel price (R$/L), then derive liters, kg and cost
         for each leg.
       - Cabotage leg → call modules.fuel.cabotage_fuel_service.get_cabotage_fuel_profile
         to get sea + ops/hotel fuel (kg).

The result is a MultimodalFuelProfile that can be serialized to JSON and used
by higher layers (heatmaps, single evaluation CLI, notebooks, etc.).

Units
-----
- distance_km: km
- cargo_t    : tonnes
- road fuel  : liters (primary) and kg (using a fixed density)
- sea fuel   : kg
- costs      : R$
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
import sys
from typing import Any, Dict, Optional, List

import json

# ───────────────────── path bootstrap (modules → repo root) ────────────────────
ROOT = Path(__file__).resolve().parents[2]  # repo root (one level above /modules)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ────────────────────────────────────────────────────────────────────────────────
# Project imports
# ────────────────────────────────────────────────────────────────────────────────
from modules.infra.logging import get_logger, init_logging
from modules.infra.database_manager import (
      db_session
    , ensure_main_table
    , list_runs
    , upsert_run
    , DEFAULT_DB_PATH
    , DEFAULT_TABLE
)

from modules.road.ors_common import ORSConfig
from modules.road.ors_client import ORSClient

from modules.addressing.resolver import (
      resolve_point_null_safe as geo_resolve
)

from modules.ports.ports_index import load_ports
from modules.ports.ports_nearest import find_nearest_port

from modules.cabotage.sea_matrix import SeaMatrix

from modules.fuel.road_fuel_service import (
      RoadFuelProfile
    , get_road_fuel_profile
)
from modules.fuel.cabotage_fuel_service import (
      CabotageFuelProfile
    , get_cabotage_fuel_profile
    , DEFAULT_HOTEL_JSON as CAB_DEFAULT_HOTEL_JSON
)

# IMPORTANT: this lives under `app/`, not `modules/app/`
from modules.app.multimodal_route_builder import (
      _ensure_road_leg
    , DEFAULT_PORTS_JSON as MM_DEFAULT_PORTS_JSON
    , DEFAULT_SEA_MATRIX_JSON as MM_DEFAULT_SEA_MATRIX_JSON
)

log = get_logger(__name__)

# ────────────────────────────────────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────────────────────────────────────

#: Fixed density to convert road diesel from L → kg (planning-level)
ROAD_DIESEL_DENSITY_KG_PER_L: float = 0.84


# ────────────────────────────────────────────────────────────────────────────────
# Data models
# ────────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RoadLegFuel:
    """
    Fuel and cost summary for a single road leg.

    kind : str
        One of: "road_only", "origin_to_port", "port_to_destiny".
    """
    kind: str
    origin_label: str
    destiny_label: str
    distance_km: Optional[float]
    cargo_t: float
    km_per_liter: Optional[float]
    diesel_price_r_per_liter: Optional[float]
    fuel_liters: Optional[float]
    fuel_kg: Optional[float]
    fuel_cost_r: Optional[float]
    cached: bool
    meta: Dict[str, Any]


@dataclass(frozen=True)
class MultimodalFuelProfile:
    """
    Full multimodal fuel summary for a single O→D pair.

    road_legs : dict
        {
          "road_only": RoadLegFuel,
          "origin_to_port": RoadLegFuel,
          "port_to_destiny": RoadLegFuel,
        }
    """
    origin_raw: str
    destiny_raw: str
    origin_label: str
    destiny_label: str
    cargo_t: float
    road_legs: Dict[str, RoadLegFuel]
    cabotage: CabotageFuelProfile
    totals: Dict[str, float]
    meta: Dict[str, Any]


# ────────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ────────────────────────────────────────────────────────────────────────────────

def _port_anchor(p: Dict[str, Any]) -> tuple[float, float]:
    """Return (lat, lon) for routing (gate if available, else main coords)."""
    gate = p.get("gate")
    if gate and isinstance(gate, dict):
        return float(gate["lat"]), float(gate["lon"])
    return float(p["lat"]), float(p["lon"])


def _safe_sum(values: List[Optional[float]]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    return sum(vals) if vals else None


def _road_leg_fuel_from_dict(
      kind: str
    , leg: Dict[str, Any]
    , *
    , cargo_t: float
    , truck_key: str
    , diesel_price_override_r_per_l: Optional[float]
) -> RoadLegFuel:
    """
    Convert a DB leg dict + fuel presets into a RoadLegFuel.

    leg is the dict returned by app.multimodal_route_builder._ensure_road_leg().
    """
    distance_km = leg.get("distance_km")
    origin_label = str(leg.get("origin_name") or leg.get("origin") or "")
    destiny_label = str(leg.get("destiny_name") or leg.get("destiny") or "")
    cached = bool(leg.get("cached"))

    if distance_km is None:
        log.warning(
              "Road leg %s (%s → %s) has NULL distance_km; fuel cannot be computed."
            , kind
            , origin_label
            , destiny_label
        )
        return RoadLegFuel(
              kind=kind
            , origin_label=origin_label
            , destiny_label=destiny_label
            , distance_km=None
            , cargo_t=float(cargo_t)
            , km_per_liter=None
            , diesel_price_r_per_liter=None
            , fuel_liters=None
            , fuel_kg=None
            , fuel_cost_r=None
            , cached=cached
            , meta={
                  "is_hgv": leg.get("is_hgv")
                , "profile_used": leg.get("profile_used")
            }
        )

    # Use road_fuel_service to obtain km/L and diesel price
    profile: RoadFuelProfile = get_road_fuel_profile(
          cargo_t=cargo_t
        , origin=origin_label
        , destiny=destiny_label
        , truck_key=truck_key
        , diesel_price_override_r_per_l=diesel_price_override_r_per_l
    )

    kmL = float(profile.km_per_liter)
    price = float(profile.diesel_price_r_per_liter)

    if kmL <= 0:
        log.warning(
              "Road leg %s (%s → %s) has non-positive km/L=%.4f; fuel cannot be computed."
            , kind
            , origin_label
            , destiny_label
            , kmL
        )
        fuel_l = None
    else:
        fuel_l = float(distance_km) / kmL

    fuel_kg = None if fuel_l is None else fuel_l * ROAD_DIESEL_DENSITY_KG_PER_L
    fuel_cost = None if fuel_l is None else fuel_l * price

    meta: Dict[str, Any] = {
          "is_hgv": leg.get("is_hgv")
        , "profile_used": leg.get("profile_used")
        , "truck_key": profile.truck_key
        , "axles": profile.axles
        , "price_source": profile.price_source
        , "extra": profile.extra
    }

    return RoadLegFuel(
          kind=kind
        , origin_label=origin_label
        , destiny_label=destiny_label
        , distance_km=float(distance_km)
        , cargo_t=float(cargo_t)
        , km_per_liter=kmL
        , diesel_price_r_per_liter=price
        , fuel_liters=fuel_l
        , fuel_kg=fuel_kg
        , fuel_cost_r=fuel_cost
        , cached=cached
        , meta=meta
    )


# ────────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────────

def get_multimodal_fuel_profile(
      *
    , origin: str
    , destiny: str
    , cargo_t: float
    , truck_key: str = "auto_by_weight"
    , diesel_price_override_r_per_l: Optional[float] = None
    , cabotage_fuel_type: str = "vlsfo"
    , include_ops_and_hotel: bool = True
    , ors_profile: str = "driving-hgv"
    , fallback_to_car: bool = True
    , overwrite: bool = False
    , db_path: Path | str = DEFAULT_DB_PATH
    , table_name: str = DEFAULT_TABLE
    , ports_json: Path | str = MM_DEFAULT_PORTS_JSON
    , sea_matrix_json: Path | str = MM_DEFAULT_SEA_MATRIX_JSON
    , hotel_json: Path | str = CAB_DEFAULT_HOTEL_JSON
) -> MultimodalFuelProfile:
    """
    High-level entry: origin/destiny + cargo_t → full fuel summary.
    """
    cargo_t = float(cargo_t)
    db_path = Path(db_path)
    ports_json = Path(ports_json)
    sea_matrix_json = Path(sea_matrix_json)
    hotel_json = Path(hotel_json)

    log.info(
          "Multimodal fuel: origin=%r destiny=%r cargo_t=%.3f truck_key=%s ors_profile=%s overwrite=%s"
        , origin
        , destiny
        , cargo_t
        , truck_key
        , ors_profile
        , overwrite
    )

    # ORS + data
    ors = ORSClient(cfg=ORSConfig())
    ports = load_ports(path=str(ports_json))
    sea_matrix = SeaMatrix.from_json_path(sea_matrix_json)

    # 1) Geocode origin/destiny
    origin_pt = geo_resolve(
          value=origin
        , ors=ors
        , log=log
    )
    destiny_pt = geo_resolve(
          value=destiny
        , ors=ors
        , log=log
    )

    if origin_pt is None or destiny_pt is None:
        log.warning(
              "At least one side could not be geocoded; inserting NULL road-only leg "
              "for raw inputs and skipping fuel calcs. origin_raw=%r destiny_raw=%r"
            , origin
            , destiny
        )
        with db_session(db_path=db_path) as conn:
            ensure_main_table(conn, table_name=table_name)
            existing = list_runs(
                  conn
                , origin=origin
                , destiny=destiny
                , is_hgv=None
                , table_name=table_name
                , limit=1
            )
            if not existing or overwrite:
                upsert_run(
                      conn
                    , origin=origin
                    , origin_lat=None
                    , origin_lon=None
                    , destiny=destiny
                    , destiny_lat=None
                    , destiny_lon=None
                    , distance_km=None
                    , is_hgv=None
                    , table_name=table_name
                )
                log.info(
                      "Inserted/overwrote NULL leg for (%s → %s) due to geocode failure."
                    , origin
                    , destiny
                )
            else:
                log.info(
                      "NULL leg for (%s → %s) already present; not inserting again."
                    , origin
                    , destiny
                )

        # Build a minimal profile
        empty_road = RoadLegFuel(
              kind="road_only"
            , origin_label=origin
            , destiny_label=destiny
            , distance_km=None
            , cargo_t=cargo_t
            , km_per_liter=None
            , diesel_price_r_per_liter=None
            , fuel_liters=None
            , fuel_kg=None
            , fuel_cost_r=None
            , cached=False
            , meta={}
        )
        dummy_cabotage = CabotageFuelProfile(
              origin_port_name=""
            , destiny_port_name=""
            , cargo_t=cargo_t
            , fuel_type=cabotage_fuel_type
            , sea_km=0.0
            , K_kg_per_tkm=0.0
            , fuel_sea_kg=0.0
            , fuel_ops_hotel_kg=0.0
            , fuel_total_kg=0.0
            , meta={"status": "geocode_failed"}
        )

        totals: Dict[str, float] = {}
        meta: Dict[str, Any] = {
              "status": "geocode_failed"
            , "db_path": str(db_path)
            , "table_name": table_name
        }

        return MultimodalFuelProfile(
              origin_raw=origin
            , destiny_raw=destiny
            , origin_label=origin
            , destiny_label=destiny
            , cargo_t=cargo_t
            , road_legs={"road_only": empty_road}
            , cabotage=dummy_cabotage
            , totals=totals
            , meta=meta
        )

    # Normal path: we have coordinates + labels
    origin_label = str(origin_pt.label or origin)
    destiny_label = str(destiny_pt.label or destiny)
    origin_lat = float(origin_pt.lat)
    origin_lon = float(origin_pt.lon)
    destiny_lat = float(destiny_pt.lat)
    destiny_lon = float(destiny_pt.lon)

    # 2) Nearest ports
    origin_port = find_nearest_port(origin_lat, origin_lon, ports)
    destiny_port = find_nearest_port(destiny_lat, destiny_lon, ports)

    oport_lat, oport_lon = _port_anchor(origin_port)
    dport_lat, dport_lon = _port_anchor(destiny_port)

    # 3) Sea distance (for info; fuel comes from cabotage_fuel_service)
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

    # 4) ROAD legs (via the same helper used by app.multimodal_route_builder)
    road_only_dict = _ensure_road_leg(
          ors
        , origin_name=origin_label
        , origin_lat=origin_lat
        , origin_lon=origin_lon
        , destiny_name=destiny_label
        , destiny_lat=destiny_lat
        , destiny_lon=destiny_lon
        , db_path=db_path
        , table_name=table_name
        , primary_profile=ors_profile
        , fallback_to_car=fallback_to_car
        , overwrite=overwrite
    )

    origin_to_port_dict = _ensure_road_leg(
          ors
        , origin_name=origin_label
        , origin_lat=origin_lat
        , origin_lon=origin_lon
        , destiny_name=origin_port["name"]
        , destiny_lat=oport_lat
        , destiny_lon=oport_lon
        , db_path=db_path
        , table_name=table_name
        , primary_profile=ors_profile
        , fallback_to_car=fallback_to_car
        , overwrite=overwrite
    )

    port_to_destiny_dict = _ensure_road_leg(
          ors
        , origin_name=destiny_port["name"]
        , origin_lat=dport_lat
        , origin_lon=dport_lon
        , destiny_name=destiny_label
        , destiny_lat=destiny_lat
        , destiny_lon=destiny_lon
        , db_path=db_path
        , table_name=table_name
        , primary_profile=ors_profile
        , fallback_to_car=fallback_to_car
        , overwrite=overwrite
    )

    # 5) Road fuel per leg
    road_only = _road_leg_fuel_from_dict(
          "road_only"
        , road_only_dict
        , cargo_t=cargo_t
        , truck_key=truck_key
        , diesel_price_override_r_per_l=diesel_price_override_r_per_l
    )
    origin_to_port = _road_leg_fuel_from_dict(
          "origin_to_port"
        , origin_to_port_dict
        , cargo_t=cargo_t
        , truck_key=truck_key
        , diesel_price_override_r_per_l=diesel_price_override_r_per_l
    )
    port_to_destiny = _road_leg_fuel_from_dict(
          "port_to_destiny"
        , port_to_destiny_dict
        , cargo_t=cargo_t
        , truck_key=truck_key
        , diesel_price_override_r_per_l=diesel_price_override_r_per_l
    )

    road_legs: Dict[str, RoadLegFuel] = {
          "road_only": road_only
        , "origin_to_port": origin_to_port
        , "port_to_destiny": port_to_destiny
    }

    # 6) Cabotage fuel (sea + ops/hotel)
    cab_profile = get_cabotage_fuel_profile(
          origin_port_name=origin_port["name"]
        , destiny_port_name=destiny_port["name"]
        , cargo_t=cargo_t
        , fuel_type=cabotage_fuel_type
        , include_ops_and_hotel=include_ops_and_hotel
        , ports_json=ports_json
        , sea_matrix_json=sea_matrix_json
        , hotel_json=hotel_json
    )

    # 7) Totals
    road_only_l = road_only.fuel_liters
    road_only_kg = road_only.fuel_kg
    road_only_cost = road_only.fuel_cost_r

    multimodal_road_l = _safe_sum([
          origin_to_port.fuel_liters
        , port_to_destiny.fuel_liters
    ])
    multimodal_road_kg = _safe_sum([
          origin_to_port.fuel_kg
        , port_to_destiny.fuel_kg
    ])
    multimodal_road_cost = _safe_sum([
          origin_to_port.fuel_cost_r
        , port_to_destiny.fuel_cost_r
    ])

    cabotage_kg = float(cab_profile.fuel_total_kg)

    totals: Dict[str, float] = {}
    if road_only_l is not None:
        totals["road_only_liters"] = road_only_l
    if road_only_kg is not None:
        totals["road_only_kg"] = road_only_kg
    if road_only_cost is not None:
        totals["road_only_cost_r"] = road_only_cost

    if multimodal_road_l is not None:
        totals["multimodal_road_liters"] = multimodal_road_l
    if multimodal_road_kg is not None:
        totals["multimodal_road_kg"] = multimodal_road_kg
    if multimodal_road_cost is not None:
        totals["multimodal_road_cost_r"] = multimodal_road_cost

    totals["cabotage_fuel_kg"] = cabotage_kg

    if multimodal_road_kg is not None:
        totals["multimodal_total_kg_equiv"] = multimodal_road_kg + cabotage_kg

    meta: Dict[str, Any] = {
          "db_path": str(db_path)
        , "table_name": table_name
        , "ports_json": str(ports_json)
        , "sea_matrix_json": str(sea_matrix_json)
        , "hotel_json": str(hotel_json)
        , "sea_distance_km": float(sea_km)
        , "sea_distance_source": sea_source
        , "origin_port": origin_port
        , "destiny_port": destiny_port
        , "status": "ok"
    }

    log.info(
          "Multimodal fuel completed for (%s → %s): road_only_l=%s, multimodal_road_l=%s, cabotage_kg=%.3f"
        , origin_label
        , destiny_label
        , "NULL" if road_only_l is None else f"{road_only_l:.3f}"
        , "NULL" if multimodal_road_l is None else f"{multimodal_road_l:.3f}"
        , cabotage_kg
    )

    return MultimodalFuelProfile(
          origin_raw=origin
        , destiny_raw=destiny
        , origin_label=origin_label
        , destiny_label=destiny_label
        , cargo_t=cargo_t
        , road_legs=road_legs
        , cabotage=cab_profile
        , totals=totals
        , meta=meta
    )


# ────────────────────────────────────────────────────────────────────────────────
# CLI / smoke test
# ────────────────────────────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Compute multimodal fuel usage (road-only vs cabotage) for a single O→D pair."
        )
    )
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
    parser.add_argument(
          "--cargo-t"
        , type=float
        , required=True
        , help="Cargo mass to move (tonnes)."
    )
    parser.add_argument(
          "--truck-key"
        , type=str
        , default="auto_by_weight"
        , help="Truck preset key (see modules.fuel.truck_specs.list_truck_keys)."
    )
    parser.add_argument(
          "--diesel-price-override"
        , type=float
        , default=None
        , help="Override diesel price [R$/L] for road legs."
    )
    parser.add_argument(
          "--cabotage-fuel-type"
        , type=str
        , default="vlsfo"
        , choices=["vlsfo", "mfo"]
        , help="Ship fuel type for sea leg."
    )

    parser.add_argument(
          "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS routing profile. Default: driving-hgv."
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
            , help="Recompute legs even if cached. Default: False"
        )
        parser.add_argument(
              "--include-ops-hotel"
            , dest="include_ops_and_hotel"
            , default=True
            , action=BooleanOptionalAction
            , help="Include port ops + hotel fuel in cabotage leg. Default: True"
        )
    except Exception:  # pragma: no cover - fallback for very old Python
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
        parser.add_argument(
              "--include-ops-hotel"
            , dest="include_ops_and_hotel"
            , action="store_true"
            , default=True
        )
        parser.add_argument(
              "--no-include-ops-hotel"
            , dest="include_ops_and_hotel"
            , action="store_false"
        )

    parser.add_argument(
          "--db-path"
        , type=Path
        , default=DEFAULT_DB_PATH
        , help=f"SQLite path. Default: {DEFAULT_DB_PATH}"
    )
    parser.add_argument(
          "--table"
        , default=DEFAULT_TABLE
        , help=f"Routes table name. Default: {DEFAULT_TABLE}"
    )
    parser.add_argument(
          "--ports-json"
        , type=Path
        , default=MM_DEFAULT_PORTS_JSON
        , help=f"Ports JSON path. Default: {MM_DEFAULT_PORTS_JSON}"
    )
    parser.add_argument(
          "--sea-matrix-json"
        , type=Path
        , default=MM_DEFAULT_SEA_MATRIX_JSON
        , help=f"Sea matrix JSON path. Default: {MM_DEFAULT_SEA_MATRIX_JSON}"
    )
    parser.add_argument(
          "--hotel-json"
        , type=Path
        , default=CAB_DEFAULT_HOTEL_JSON
        , help=f"Hotel factors JSON path. Default: {CAB_DEFAULT_HOTEL_JSON}"
    )

    parser.add_argument(
          "--log-level"
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    parser.add_argument(
          "--pretty"
        , action="store_true"
        , help="Pretty-print JSON output."
    )

    args = parser.parse_args(argv)

    init_logging(
          level=args.log_level
        , force=True
        , write_output=False
    )

    log.info(
          "CLI multimodal fuel call: origin=%r destiny=%r cargo_t=%.3f truck_key=%s"
        , args.origin
        , args.destiny
        , args.cargo_t
        , args.truck_key
    )

    profile = get_multimodal_fuel_profile(
          origin=args.origin
        , destiny=args.destiny
        , cargo_t=args.cargo_t
        , truck_key=args.truck_key
        , diesel_price_override_r_per_l=args.diesel_price_override
        , cabotage_fuel_type=args.cabotage_fuel_type
        , include_ops_and_hotel=args.include_ops_and_hotel
        , ors_profile=args.ors_profile
        , fallback_to_car=args.fallback_to_car
        , overwrite=args.overwrite
        , db_path=args.db_path
        , table_name=args.table
        , ports_json=args.ports_json
        , sea_matrix_json=args.sea_matrix_json
        , hotel_json=args.hotel_json
    )

    payload = {
          "origin_raw": profile.origin_raw
        , "destiny_raw": profile.destiny_raw
        , "origin_label": profile.origin_label
        , "destiny_label": profile.destiny_label
        , "cargo_t": profile.cargo_t
        , "road_legs": {
              k: asdict(v)
            for k, v in profile.road_legs.items()
        }
        , "cabotage": asdict(profile.cabotage)
        , "totals": profile.totals
        , "meta": profile.meta
    }

    if args.pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
