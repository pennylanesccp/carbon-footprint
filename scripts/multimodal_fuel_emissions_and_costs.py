#!/usr/bin/env python3
# scripts/multimodal_fuel_emissions_and_costs.py
# -*- coding: utf-8 -*-
"""
Multimodal fuel + emissions + costs comparer (road-only vs cabotage)
====================================================================

Given:
  - origin  (string address / CEP / "lat,lon")
  - destiny (string address / CEP / "lat,lon")
  - cargo mass (t)

This script will:

  1) Call modules.fuel.multimodal_fuel_service.get_multimodal_fuel_profile(...)
     to build a consistent multimodal fuel profile:
       - road-only leg
       - origin → port road leg
       - port → destiny road leg
       - cabotage leg (sea + ops + hotel)

  2) Use modules.fuel.emissions.estimate_fuel_emissions(...) to compute CO₂e:
       - road-only (diesel)
       - multimodal road part (diesel)
       - sea leg (VLSFO / MGO / MFO)

  3) Fetch ship fuel prices for Santos (VLSFO, MGO) from Ship & Bunker and
     convert them to BRL using modules.costs.ship_fuel_prices.apply_fx_brl.
     From that it estimates the sea fuel cost (R$).

  4) Emit a JSON payload summarising:

       {
         "origin_raw": ...,
         "destiny_raw": ...,
         "origin_label": ...,
         "destiny_label": ...,
         "cargo_t": ...,
         "scenarios": {
           "road_only": {...},
           "multimodal": {
             "road": {...},
             "sea": {...},
             "totals": {...}
           }
         },
         "pricing_sources": {...},
         "raw": {...}  # original multimodal_fuel_service payload
       }

Example (PowerShell)
--------------------

python -m scripts.multimodal_fuel_emissions_and_costs `
    --origin "São Paulo, SP" `
    --destiny "Salvador, BA" `
    --cargo-t 30 `
    --pretty
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import json
import sys
from typing import Any, Dict, Optional, List

# ───────────────────── path bootstrap (scripts → repo root) ────────────────────
ROOT = Path(__file__).resolve().parents[1]  # repo root (one level above /scripts)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ────────────────────────────────────────────────────────────────────────────────
# Project imports
# ────────────────────────────────────────────────────────────────────────────────
from modules.infra.logging import get_logger, init_logging

from modules.fuel.multimodal_fuel_service import (
      get_multimodal_fuel_profile
    , MultimodalFuelProfile
)

from modules.fuel.emissions import (
      estimate_fuel_emissions
)

from modules.costs.ship_fuel_prices import (
      fetch_santos_prices
    , apply_fx_brl
)

from modules.infra.database_manager import (
      DEFAULT_DB_PATH
    , DEFAULT_TABLE as DEFAULT_DISTANCE_TABLE
)

from modules.fuel.cabotage_fuel_service import (
      DEFAULT_HOTEL_JSON as CAB_DEFAULT_HOTEL_JSON
)

from modules.app.multimodal_route_builder import (
      DEFAULT_PORTS_JSON as MM_DEFAULT_PORTS_JSON
    , DEFAULT_SEA_MATRIX_JSON as MM_DEFAULT_SEA_MATRIX_JSON
)

log = get_logger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ────────────────────────────────────────────────────────────────────────────────

def _safe_float(value: Any) -> Optional[float]:
    """
    Convert to float, preserving None.
    """
    if value is None:
        return None
    return float(value)


def _compute_road_only_block(
    profile: MultimodalFuelProfile
) -> Dict[str, Any]:
    """
    Build the 'road_only' scenario block.

    Uses diesel EF via estimate_fuel_emissions(fuel_kg, "diesel").
    """
    road_only = profile.road_legs.get("road_only")
    if road_only is None:
        return {
              "distance_km": None
            , "fuel_liters": None
            , "fuel_kg": None
            , "fuel_cost_r": None
            , "co2e_kg": None
        }

    distance_km = _safe_float(road_only.distance_km)
    fuel_liters = _safe_float(road_only.fuel_liters)
    fuel_kg     = _safe_float(road_only.fuel_kg)
    fuel_cost_r = _safe_float(road_only.fuel_cost_r)

    co2e_kg: Optional[float] = None
    if fuel_kg is not None:
        em = estimate_fuel_emissions(
              fuel_mass_kg=fuel_kg
            , fuel_type="diesel"
        )
        co2e_kg = float(em["co2e_kg"])

    return {
          "distance_km": distance_km
        , "fuel_liters": fuel_liters
        , "fuel_kg": fuel_kg
        , "fuel_cost_r": fuel_cost_r
        , "co2e_kg": co2e_kg
    }


def _compute_multimodal_blocks(
    profile: MultimodalFuelProfile
) -> Dict[str, Any]:
    """
    Build the 'multimodal' block (road + sea + totals), *without* sea cost.

    Emissions:
      - road part: diesel
      - sea part: profile.cabotage.fuel_type (e.g. "vlsfo")
    """
    totals_dict: Dict[str, Any] = dict(profile.totals or {})

    road_fuel_liters = _safe_float(totals_dict.get("multimodal_road_liters"))
    road_fuel_kg     = _safe_float(totals_dict.get("multimodal_road_kg"))
    road_fuel_cost_r = _safe_float(totals_dict.get("multimodal_road_cost_r"))

    co2e_road_kg: Optional[float] = None
    if road_fuel_kg is not None:
        em_road = estimate_fuel_emissions(
              fuel_mass_kg=road_fuel_kg
            , fuel_type="diesel"
        )
        co2e_road_kg = float(em_road["co2e_kg"])

    sea_fuel_kg = float(profile.cabotage.fuel_total_kg)
    em_sea = estimate_fuel_emissions(
          fuel_mass_kg=sea_fuel_kg
        , fuel_type=profile.cabotage.fuel_type
    )
    co2e_sea_kg = float(em_sea["co2e_kg"])

    # multimodal totals
    if road_fuel_kg is not None:
        multimodal_total_fuel_kg = road_fuel_kg + sea_fuel_kg
        if co2e_road_kg is not None:
            multimodal_total_co2e_kg = co2e_road_kg + co2e_sea_kg
        else:
            multimodal_total_co2e_kg = None
    else:
        multimodal_total_fuel_kg = sea_fuel_kg
        multimodal_total_co2e_kg = co2e_sea_kg

    sea_block: Dict[str, Any] = {
          "sea_km": float(profile.cabotage.sea_km)
        , "fuel_kg": sea_fuel_kg
        , "fuel_cost_r": None      # filled later when ship prices are available
        , "co2e_kg": co2e_sea_kg
        , "fuel_type": profile.cabotage.fuel_type
    }

    road_block: Dict[str, Any] = {
          "fuel_liters": road_fuel_liters
        , "fuel_kg": road_fuel_kg
        , "fuel_cost_r": road_fuel_cost_r
        , "co2e_kg": co2e_road_kg
    }

    totals_block: Dict[str, Any] = {
          "fuel_kg": multimodal_total_fuel_kg
        , "fuel_cost_r": None      # filled later when sea cost is known
        , "co2e_kg": multimodal_total_co2e_kg
        , "delta_co2e_vs_road_only_kg": None   # filled later once road_only known
        , "delta_cost_vs_road_only_r": None
    }

    return {
          "road": road_block
        , "sea": sea_block
        , "totals": totals_block
    }


def _attach_ship_fuel_cost(
      multimodal_block: Dict[str, Any]
    , profile: MultimodalFuelProfile
) -> Dict[str, Any]:
    """
    Enrich 'multimodal' block with sea fuel cost using Ship & Bunker prices.

    Returns a dict describing the ship fuel pricing source to be placed under
    payload["pricing_sources"]["ship_fuel"].
    """
    sea_block    = multimodal_block["sea"]
    totals_block = multimodal_block["totals"]

    sea_fuel_kg = _safe_float(sea_block.get("fuel_kg"))
    if sea_fuel_kg is None or sea_fuel_kg <= 0.0:
        log.info("Sea fuel kg is zero or NULL; skipping ship fuel pricing.")
        return {
              "status": "skipped"
            , "reason": "sea_fuel_kg_null_or_zero"
        }

    fuel_type = str(sea_block.get("fuel_type") or "").lower()

    try:
        # 1) Fetch Santos bunker prices in USD/mt
        prices_usd = fetch_santos_prices()

        # 2) Apply FX using the helper's built-in converter
        #    (it will call the BCB/ECB helper internally).
        #    IMPORTANT: do NOT pass usd_brl_rate here; signature is:
        #        apply_fx_brl(prices: Dict[str, Any], converter=None) -> Dict[str, Any]
        prices_brl = apply_fx_brl(prices_usd)

        # 3) Select BRL/mt for the fuel type in use.
        #    Ship & Bunker gives us VLSFO + MGO; when using "mfo", we
        #    approximate with VLSFO price (planning-level simplification).
        key_map = {
              "vlsfo": "vlsfo_brl_per_mt"
            , "mgo":   "mgo_brl_per_mt"
            , "mfo":   "vlsfo_brl_per_mt"
        }
        price_key = key_map.get(fuel_type)

        if not price_key or prices_brl.get(price_key) is None:
            log.warning(
                  "No matching BRL price key for fuel_type=%r (price_key=%r); "
                  "sea fuel cost will remain NULL."
                , fuel_type
                , price_key
            )
            # still return all pricing info we have
            data = dict(prices_brl)
            data["status"] = "ok"
            data["note"] = "prices_fetched_but_no_matching_key"
            return data

        price_brl_per_mt = float(prices_brl[price_key])
        fuel_mt          = sea_fuel_kg / 1000.0
        sea_cost_r       = fuel_mt * price_brl_per_mt

        sea_block["fuel_cost_r"] = sea_cost_r

        # If road part has cost, add it for multimodal total cost
        road_cost_r = _safe_float(multimodal_block["road"].get("fuel_cost_r"))
        totals_block["fuel_cost_r"] = (
            None if road_cost_r is None else road_cost_r + sea_cost_r
        )

        data = dict(prices_brl)
        data["status"] = "ok"
        return data

    except Exception as exc:  # pragma: no cover - network/runtime failures
        log.error(
              "Failed to fetch/apply ship fuel prices for cost calculation; "
              "cabotage cost will be NULL. err=%s"
            , exc
        )
        return {
              "status": "error"
            , "error": str(exc)
        }


def _build_payload(
    profile: MultimodalFuelProfile
) -> Dict[str, Any]:
    """
    Build final JSON-serialisable payload from MultimodalFuelProfile.
    """
    road_only_block   = _compute_road_only_block(profile)
    multimodal_block  = _compute_multimodal_blocks(profile)

    # Attach ship fuel cost and pricing sources
    ship_pricing = _attach_ship_fuel_cost(
          multimodal_block=multimodal_block
        , profile=profile
    )

    # Deltas vs road-only (only if both sides have CO₂ and cost)
    road_only_co2e_kg = _safe_float(road_only_block.get("co2e_kg"))
    road_only_cost_r  = _safe_float(road_only_block.get("fuel_cost_r"))

    totals_block      = multimodal_block["totals"]
    multi_co2e_kg     = _safe_float(totals_block.get("co2e_kg"))
    multi_cost_r      = _safe_float(totals_block.get("fuel_cost_r"))

    if road_only_co2e_kg is not None and multi_co2e_kg is not None:
        totals_block["delta_co2e_vs_road_only_kg"] = (
            multi_co2e_kg - road_only_co2e_kg
        )

    if road_only_cost_r is not None and multi_cost_r is not None:
        totals_block["delta_cost_vs_road_only_r"] = (
            multi_cost_r - road_only_cost_r
        )

    # Pricing sources
    road_only_leg = profile.road_legs.get("road_only")
    road_diesel_price = None
    if road_only_leg is not None:
        road_diesel_price = _safe_float(road_only_leg.diesel_price_r_per_liter)

    pricing_sources: Dict[str, Any] = {
          "road_diesel_price_r_per_liter": road_diesel_price
        , "ship_fuel": ship_pricing
    }

    raw_block: Dict[str, Any] = {
          "road_legs": {
                k: asdict(v)
            for k, v in profile.road_legs.items()
        }
        , "cabotage": asdict(profile.cabotage)
        , "totals": profile.totals
        , "meta": profile.meta
    }

    return {
          "origin_raw": profile.origin_raw
        , "destiny_raw": profile.destiny_raw
        , "origin_label": profile.origin_label
        , "destiny_label": profile.destiny_label
        , "cargo_t": profile.cargo_t
        , "scenarios": {
              "road_only": road_only_block
            , "multimodal": multimodal_block
        }
        , "pricing_sources": pricing_sources
        , "raw": raw_block
    }


# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Compute multimodal fuel usage, emissions and costs "
            "(road-only vs cabotage) for a single O→D pair."
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

    # Boolean flags (Python 3.9+ has BooleanOptionalAction; keep fallback)
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
    except Exception:  # pragma: no cover - very old Python fallback
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
          "--distance-table"
        , default=DEFAULT_DISTANCE_TABLE
        , help=f"Road legs cache table name. Default: {DEFAULT_DISTANCE_TABLE}"
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
          "CLI multimodal fuel+emissions+costs: origin=%r destiny=%r cargo_t=%.3f truck_key=%s"
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
        , table_name=args.distance_table
        , ports_json=args.ports_json
        , sea_matrix_json=args.sea_matrix_json
        , hotel_json=args.hotel_json
    )

    payload = _build_payload(profile)

    if args.pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
