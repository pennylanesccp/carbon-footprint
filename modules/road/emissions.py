# -*- coding: utf-8 -*-
"""
Road emissions/cost estimator — wrapper around axle-based fuel model.
Keeps the original API used by modules/app/evaluator.py:
    estimate_road_trip(distance_km, cargo_t, diesel_price_brl_per_l, spec, empty_backhaul_share)
and exposes TRUCK_SPECS.
"""

from __future__ import annotations

from typing import Any, Dict, Union

from .truck_specs import TRUCK_SPECS
from .fuel_model import estimate_leg_liters

# Basic factors (planning values)
DIESEL_DENSITY_KG_PER_L: float = 0.84
EF_DIESEL_CO2_KG_PER_L : float = 2.68   # tailpipe CO2 per liter (planning)

def _resolve_spec(spec: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(spec, str):
        if spec not in TRUCK_SPECS:
            raise KeyError(f"Unknown truck preset '{spec}'.")
        return dict(TRUCK_SPECS[spec])    # copy
    if isinstance(spec, dict):
        return dict(spec)
    raise TypeError("spec must be a preset name (str) or a spec dict.")

def estimate_road_trip(
      *
    , distance_km: float
    , cargo_t: float
    , diesel_price_brl_per_l: float
    , spec: Union[str, Dict[str, Any]]
    , empty_backhaul_share: float = 0.0
) -> Dict[str, Any]:
    """
    Returns a dict with 'fuel', 'emissions', 'cost'.
    Compatible with your existing evaluator.
    """
    _spec = _resolve_spec(spec)

    ( liters_total
    , liters_loaded
    , liters_empty
    , trips
    , kmL_loaded
    , kmL_empty
    ) = estimate_leg_liters(
          distance_km=distance_km
        , cargo_t=cargo_t
        , spec=_spec
        , empty_backhaul_share=float(empty_backhaul_share)
    )

    fuel_cost_brl = float(liters_total) * float(diesel_price_brl_per_l)
    co2_kg        = float(liters_total) * EF_DIESEL_CO2_KG_PER_L

    return {
          "fuel": {
              "liters_total": liters_total
            , "liters_loaded": liters_loaded
            , "liters_empty": liters_empty
            , "trips_total": trips
            , "kmL_loaded": kmL_loaded
            , "kmL_empty": kmL_empty
        }
        , "emissions": {
              "co2_kg": co2_kg
            , "co2e_total_kg": co2_kg   # until you add CH4/N2O
        }
        , "cost": {
              "fuel_cost_brl": fuel_cost_brl
        }
        , "spec_used": _spec
    }

__all__ = ["TRUCK_SPECS", "estimate_road_trip", "DIESEL_DENSITY_KG_PER_L"]
