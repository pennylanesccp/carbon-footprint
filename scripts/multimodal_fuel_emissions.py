#!/usr/bin/env python3
# scripts/multimodal_fuel_emissions.py
# -*- coding: utf-8 -*-
"""
Multimodal fuel, cost & emissions evaluator
==========================================

Given:
  - origin (text or 'lat,lon')
  - destiny (text or 'lat,lon')
  - cargo mass (t)

This script will:

  1) Call modules.fuel.multimodal_fuel_service.get_multimodal_fuel_profile(...)
     to compute:
       - road-only leg (origin → destiny)
       - multimodal legs (origin → origin_port, origin_port → destiny_port (sea), destiny_port → destiny)
       - diesel liters/kg and BRL cost for road legs
       - fuel mass (kg) for cabotage leg (sea + ops/hotel)

  2) Estimate emissions:
       - Road diesel TTW CO2e using a fixed EF per liter.
       - Ship fuel TTW CO2e using EFs per kg fuel (by fuel_type: vlsfo/mfo/mgo).

  3) Estimate cabotage fuel cost:
       - Fetch latest prices for Santos from Ship & Bunker (via modules.costs.ship_fuel_prices).
       - Convert USD/mt → BRL/mt (FX either from CLI or internal fallback).
       - Derive BRL per kg and multiply by cabotage fuel mass (kg).

Output
------
Prints a single JSON object with keys:

{
  "origin_raw": ...,
  "destiny_raw": ...,
  "origin_label": ...,
  "destiny_label": ...,
  "cargo_t": ...,
  "scenarios": {
    "road_only": {
      "distance_km": ...,
      "fuel_liters": ...,
      "fuel_kg": ...,
      "fuel_cost_r": ...,
      "co2e_kg": ...,
    },
    "multimodal": {
      "road": {...},
      "sea": {...},
      "totals": {
        "fuel_kg": ...,
        "fuel_cost_r": ...,
        "co2e_kg": ...,
        "delta_co2e_vs_road_only_kg": ...,
        "delta_cost_vs_road_only_r": ...
      }
    }
  },
  "pricing_sources": {
    "road_diesel_price_r_per_liter": ...,
    "ship_fuel": {...}   # Ship & Bunker, FX, etc.
  },
  "raw": {
    "road_legs": {...},  # asdict(RoadLegFuel)
    "cabotage": {...},   # asdict(CabotageFuelProfile)
    "totals": {...},     # profile.totals
    "meta": {...}        # profile.meta
  }
}
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import sys

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
)
from modules.costs.ship_fuel_prices import (
      fetch_santos_prices
    , apply_fx_brl
)

log = get_logger(__name__)

# ────────────────────────────────────────────────────────────────────────────────
# Emission factors & helpers
# ────────────────────────────────────────────────────────────────────────────────

#: Diesel TTW emission factor (kg CO2e per liter).
EF_DIESEL_TTW_CO2E_KG_PER_L: float = 2.68

#: Simple TTW emission factors for marine fuels (kg CO2e per kg fuel).
FUEL_EF_CO2E_KG_PER_KG: Dict[str, float] = {
      "vlsfo": 3.114  # Very Low Sulphur Fuel Oil (approximate)
    , "mfo": 3.114   # Treat MFO ~ residual HFO for now (same as VLSFO; adjust if needed)
    , "mgo": 3.206   # Marine Gas Oil (approximate)
    , "diesel": 3.190  # Back-calculated: 2.68 kg/L @ 0.84 kg/L ≈ 3.19 kg/kg
}


def _compute_road_co2e_kg(liters: Optional[float]) -> Optional[float]:
    """Return road diesel CO2e (kg) from liters, or None if liters is None."""
    if liters is None:
        return None
    return float(liters) * EF_DIESEL_TTW_CO2E_KG_PER_L


def _compute_ship_co2e_kg(
      fuel_kg: Optional[float]
    , fuel_type: str
) -> Optional[float]:
    """Return ship fuel CO2e (kg) from kg fuel and fuel_type, or None."""
    if fuel_kg is None:
        return None

    key = fuel_type.strip().lower()
    ef = FUEL_EF_CO2E_KG_PER_KG.get(key)
    if ef is None:
        log.warning(
              "Unknown ship fuel_type=%r for emissions; known=%s → emissions set to NULL."
            , fuel_type
            , sorted(FUEL_EF_CO2E_KG_PER_KG.keys())
        )
        return None

    return float(fuel_kg) * float(ef)


def _estimate_ship_fuel_cost_brl(
      fuel_kg: Optional[float]
    , fuel_type: str
    , usd_brl_rate: Optional[float] = None
) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Estimate cabotage fuel cost in BRL given fuel mass and fuel type.

    Uses Ship & Bunker prices for Santos (VLSFO / MGO) and converts
    USD/mt → BRL/mt → BRL/kg.

    Parameters
    ----------
    fuel_kg : Optional[float]
        Total fuel mass used in cabotage leg (kg).
    fuel_type : str
        Usually 'vlsfo' or 'mfo' (we map 'mfo' to the MGO price as a proxy).
    usd_brl_rate : Optional[float]
        FX rate to override Ship & Bunker USD→BRL. If None, the helper
        will use its own documented fallback.

    Returns
    -------
    (fuel_cost_brl, meta)
    """
    if fuel_kg is None:
        meta = {
              "status": "no_fuel"
            , "reason": "fuel_kg is None"
        }
        return None, meta

    if fuel_kg <= 0.0:
        meta = {
              "status": "zero_fuel"
            , "reason": "fuel_kg <= 0.0"
        }
        return 0.0, meta

    try:
        raw = fetch_santos_prices()
        prices_brl = apply_fx_brl(
              prices=raw
            , usd_brl_rate=usd_brl_rate
        )
    except Exception as exc:  # pragma: no cover - defensive
        log.error(
              "Failed to fetch/apply ship fuel prices for cost calculation; "
              "cabotage cost will be NULL. err=%s"
            , exc
        )
        meta = {
              "status": "error"
            , "error": str(exc)
        }
        return None, meta

    key = fuel_type.strip().lower()
    if key == "vlsfo":
        brl_per_mt = float(prices_brl["vlsfo_brl_per_mt"])
        price_source = "vlsfo_brl_per_mt"
    elif key in {"mfo", "mgo"}:
        # We do not have MFO directly; use MGO as a proxy.
        brl_per_mt = float(prices_brl["mgo_brl_per_mt"])
        price_source = "mgo_brl_per_mt"
    else:
        log.warning(
              "Unknown ship fuel_type=%r for cost; known=('vlsfo','mfo','mgo') → cabotage cost NULL."
            , fuel_type
        )
        meta = {
              "status": "unknown_fuel_type"
            , "fuel_type": fuel_type
            , "known_types": ["vlsfo", "mfo", "mgo"]
        }
        return None, meta

    brl_per_kg = brl_per_mt / 1000.0
    fuel_cost_brl = float(fuel_kg) * brl_per_kg

    meta = {
          "status": "ok"
        , "fuel_type": fuel_type
        , "fuel_kg": float(fuel_kg)
        , "brl_per_mt": brl_per_mt
        , "brl_per_kg": brl_per_kg
        , "price_source": price_source
        , "ship_bunker_meta": {
              "date_label": raw.get("date_label")
            , "vlsfo_usd_per_mt": raw.get("vlsfo_usd_per_mt")
            , "mgo_usd_per_mt": raw.get("mgo_usd_per_mt")
            , "usd_brl_rate": prices_brl.get("usd_brl_rate")
        }
    }
    return fuel_cost_brl, meta


# ────────────────────────────────────────────────────────────────────────────────
# CLI / main
# ────────────────────────────────────────────────────────────────────────────────

def parse_cli_args(argv: Optional[list[str]] = None):
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Run multimodal routing (road-only vs cabotage) and compute "
            "fuel, cost and emissions for a single O→D pair."
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
        , help="Override diesel price [R$/L] for road legs (optional)."
    )
    parser.add_argument(
          "--cabotage-fuel-type"
        , type=str
        , default="vlsfo"
        , choices=["vlsfo", "mfo"]
        , help="Ship fuel type for sea leg (for both fuel K and CO2e)."
    )
    parser.add_argument(
          "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS routing profile. Default: driving-hgv."
    )

    # Ship fuel cost: FX override
    parser.add_argument(
          "--usd-brl"
        , type=float
        , default=None
        , help=(
            "Optional USD/BRL FX rate for Ship & Bunker prices. "
            "If omitted, an internal default is used."
        )
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

    # Pass-throughs to multimodal_fuel_service
    parser.add_argument(
          "--db-path"
        , type=Path
        , default=None
        , help="Optional override for SQLite path used in routes cache."
    )
    parser.add_argument(
          "--table"
        , default=None
        , help="Optional override for routes table name."
    )
    parser.add_argument(
          "--ports-json"
        , type=Path
        , default=None
        , help="Optional override for ports JSON path."
    )
    parser.add_argument(
          "--sea-matrix-json"
        , type=Path
        , default=None
        , help="Optional override for sea matrix JSON path."
    )
    parser.add_argument(
          "--hotel-json"
        , type=Path
        , default=None
        , help="Optional override for hotel factors JSON path."
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

    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_cli_args(argv)

    init_logging(
          level=args.log_level
        , force=True
        , write_output=False
    )

    log.info(
          "CLI multimodal fuel+emissions: origin=%r destiny=%r cargo_t=%.3f truck_key=%s"
        , args.origin
        , args.destiny
        , args.cargo_t
        , args.truck_key
    )

    # Build kwargs for multimodal service, respecting optional overrides
    mm_kwargs: Dict[str, Any] = {
          "origin": args.origin
        , "destiny": args.destiny
        , "cargo_t": args.cargo_t
        , "truck_key": args.truck_key
        , "diesel_price_override_r_per_l": args.diesel_price_override
        , "cabotage_fuel_type": args.cabotage_fuel_type
        , "include_ops_and_hotel": args.include_ops_and_hotel
        , "ors_profile": args.ors_profile
        , "fallback_to_car": args.fallback_to_car
        , "overwrite": args.overwrite
    }

    if args.db_path is not None:
        mm_kwargs["db_path"] = args.db_path
    if args.table is not None:
        mm_kwargs["table_name"] = args.table
    if args.ports_json is not None:
        mm_kwargs["ports_json"] = args.ports_json
    if args.sea_matrix_json is not None:
        mm_kwargs["sea_matrix_json"] = args.sea_matrix_json
    if args.hotel_json is not None:
        mm_kwargs["hotel_json"] = args.hotel_json

    profile = get_multimodal_fuel_profile(**mm_kwargs)

    # If geocode failed, just echo profile structure with status and bail
    status = profile.meta.get("status")
    if status != "ok":
        log.warning(
              "Multimodal fuel profile status != 'ok' (%s); fuel/cost/emissions will be mostly NULL."
            , status
        )
        payload = {
              "origin_raw": profile.origin_raw
            , "destiny_raw": profile.destiny_raw
            , "origin_label": profile.origin_label
            , "destiny_label": profile.destiny_label
            , "cargo_t": profile.cargo_t
            , "scenarios": {}
            , "pricing_sources": {}
            , "raw": {
                  "road_legs": {k: asdict(v) for k, v in profile.road_legs.items()}
                , "cabotage": asdict(profile.cabotage)
                , "totals": dict(profile.totals)
                , "meta": dict(profile.meta)
            }
        }
        if args.pretty:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0

    # ── 1) Read base fuel + cost from profile.totals ───────────────────────────
    totals = profile.totals

    road_only_l = totals.get("road_only_liters")
    road_only_kg = totals.get("road_only_kg")
    road_only_cost_r = totals.get("road_only_cost_r")

    multimodal_road_l = totals.get("multimodal_road_liters")
    multimodal_road_kg = totals.get("multimodal_road_kg")
    multimodal_road_cost_r = totals.get("multimodal_road_cost_r")

    cabotage_kg = totals.get("cabotage_fuel_kg")

    # ── 2) Emissions ───────────────────────────────────────────────────────────
    road_only_co2e_kg = _compute_road_co2e_kg(road_only_l)
    multimodal_road_co2e_kg = _compute_road_co2e_kg(multimodal_road_l)
    cabotage_co2e_kg = _compute_ship_co2e_kg(
          fuel_kg=cabotage_kg
        , fuel_type=profile.cabotage.fuel_type
    )

    # Multimodal total emissions (road legs + cabotage)
    multimodal_total_co2e_kg: Optional[float] = None
    if multimodal_road_co2e_kg is not None and cabotage_co2e_kg is not None:
        multimodal_total_co2e_kg = multimodal_road_co2e_kg + cabotage_co2e_kg

    # ── 3) Cabotage fuel cost using Ship & Bunker ─────────────────────────────
    cabotage_cost_r, ship_pricing_meta = _estimate_ship_fuel_cost_brl(
          fuel_kg=cabotage_kg
        , fuel_type=profile.cabotage.fuel_type
        , usd_brl_rate=args.usd_brl
    )

    # Multimodal total fuel cost
    multimodal_total_cost_r: Optional[float] = None
    if multimodal_road_cost_r is not None and cabotage_cost_r is not None:
        multimodal_total_cost_r = multimodal_road_cost_r + cabotage_cost_r

    # Deltas vs road-only
    delta_co2e_vs_road_only_kg: Optional[float] = None
    if road_only_co2e_kg is not None and multimodal_total_co2e_kg is not None:
        delta_co2e_vs_road_only_kg = multimodal_total_co2e_kg - road_only_co2e_kg

    delta_cost_vs_road_only_r: Optional[float] = None
    if road_only_cost_r is not None and multimodal_total_cost_r is not None:
        delta_cost_vs_road_only_r = multimodal_total_cost_r - road_only_cost_r

    # ── 4) Build JSON payload ─────────────────────────────────────────────────
    road_only_leg = profile.road_legs.get("road_only")

    scenarios: Dict[str, Any] = {
        "road_only": {
              "distance_km": None if road_only_leg is None else road_only_leg.distance_km
            , "fuel_liters": road_only_l
            , "fuel_kg": road_only_kg
            , "fuel_cost_r": road_only_cost_r
            , "co2e_kg": road_only_co2e_kg
        },
        "multimodal": {
            "road": {
                  "fuel_liters": multimodal_road_l
                , "fuel_kg": multimodal_road_kg
                , "fuel_cost_r": multimodal_road_cost_r
                , "co2e_kg": multimodal_road_co2e_kg
            },
            "sea": {
                  "sea_km": float(profile.cabotage.sea_km)
                , "fuel_kg": cabotage_kg
                , "fuel_cost_r": cabotage_cost_r
                , "co2e_kg": cabotage_co2e_kg
                , "fuel_type": profile.cabotage.fuel_type
            },
            "totals": {
                  "fuel_kg": None
                    if multimodal_road_kg is None or cabotage_kg is None
                    else multimodal_road_kg + cabotage_kg
                , "fuel_cost_r": multimodal_total_cost_r
                , "co2e_kg": multimodal_total_co2e_kg
                , "delta_co2e_vs_road_only_kg": delta_co2e_vs_road_only_kg
                , "delta_cost_vs_road_only_r": delta_cost_vs_road_only_r
            }
        }
    }

    road_diesel_price_r_per_liter: Optional[float] = None
    if road_only_leg is not None and road_only_leg.diesel_price_r_per_liter is not None:
        road_diesel_price_r_per_liter = float(road_only_leg.diesel_price_r_per_liter)

    pricing_sources: Dict[str, Any] = {
          "road_diesel_price_r_per_liter": road_diesel_price_r_per_liter
        , "ship_fuel": ship_pricing_meta
    }

    payload = {
          "origin_raw": profile.origin_raw
        , "destiny_raw": profile.destiny_raw
        , "origin_label": profile.origin_label
        , "destiny_label": profile.destiny_label
        , "cargo_t": profile.cargo_t
        , "scenarios": scenarios
        , "pricing_sources": pricing_sources
        , "raw": {
              "road_legs": {k: asdict(v) for k, v in profile.road_legs.items()}
            , "cabotage": asdict(profile.cabotage)
            , "totals": dict(profile.totals)
            , "meta": dict(profile.meta)
        }
    }

    if args.pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
