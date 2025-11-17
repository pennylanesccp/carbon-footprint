# modules/road/emissions.py
# -*- coding: utf-8 -*-
"""
Road emissions & fuel-cost estimator
====================================

Exposes:
- estimate_road_trip(...): computes diesel liters (via fuel_model), BRL fuel cost (if price is given),
  and CO2e using a documented TTW default.

Return shape (compatible with evaluator):
{
  "fuel": {"liters_total": ...},
  "cost": {"fuel_cost_brl": ...},
  "emissions": {"co2e_total_kg": ...},
  "meta": {...}   # helpful diagnostics
}

Assumptions
-----------
- Diesel tailpipe (TTW) default EF = 2.68 kg CO2e / L.
  This covers CO2; CH4/N2O are not inflated unless explicitly passed in a custom EF.
- **Do not** mix MGO factors here; sea leg handled separately by the cabotage module.
"""

from __future__ import annotations

from typing import Dict, Any, Optional

from modules.infra.logging import get_logger
from modules.fuel.road_fuel_model import estimate_leg_liters, get_km_l_baseline
from modules.fuel.truck_specs import get_truck_spec, guess_axles_from_payload

_log = get_logger(__name__)

# Sensible, documented default (diesel TTW). Keep configurable via argument.
DEFAULT_EF_DIESEL_TTW_CO2E_KG_PER_L = 2.68


def _resolve_spec(truck_key: Optional[str], truck_spec: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Resolve a truck spec from either a known key or a provided dict.
    If truck_key == 'auto_by_weight', we still use the dict but will infer axles later.
    """
    base: Dict[str, Any] = {}
    if truck_key and truck_key != "auto_by_weight":
        try:
            base = dict(get_truck_spec(truck_key))
        except Exception as e:
            _log.warning(f"_resolve_spec: get_truck_spec('{truck_key}') failed → using provided spec only. err={e}")
    if truck_spec:
        base.update(truck_spec)
    # Provide robust defaults
    base.setdefault("payload_t", 27.0)
    base.setdefault("ref_weight_t", 20.0)
    base.setdefault("empty_efficiency_gain", 0.18)
    base.setdefault("label", truck_key or base.get("label", "road-truck"))
    return base


def estimate_road_trip(
    *,
    distance_km: float,
    cargo_t: float,
    truck_key: Optional[str] = None,
    truck_spec: Optional[Dict[str, Any]] = None,
    empty_backhaul_share: float = 0.0,
    diesel_price_brl_l: Optional[float] = None,
    elasticity: float = 1.0,
    ef_co2e_per_l: float = DEFAULT_EF_DIESEL_TTW_CO2E_KG_PER_L,
) -> Dict[str, Any]:
    """
    Estimate liters, fuel cost, and CO2e for a single road leg.

    Behavior
    --------
    - If truck_key == 'auto_by_weight' **or** spec lacks 'axles', infer axles via payload (`guess_axles_from_payload`).
    - Liters are computed via `fuel_model.estimate_leg_liters`.
    - Fuel cost = liters_total * diesel_price_brl_l (if provided).
    - Emissions CO2e = liters_total * ef_co2e_per_l (TTW).

    Parameters
    ----------
    distance_km : float
    cargo_t : float
    truck_key : Optional[str]
        Known key from truck_specs (e.g., 'semi_27t'). Special value: 'auto_by_weight'.
    truck_spec : Optional[Dict[str, Any]]
        Dict with fields like payload_t, axles (optional), ref_weight_t, empty_efficiency_gain.
        When provided together with `truck_key`, this dict overrides fields from the keyed spec.
    empty_backhaul_share : float
        Fraction of trips returning empty (0–1).
    diesel_price_brl_l : Optional[float]
        Average diesel price to apply; if None, fuel_cost_brl will be 0 and we log that.
    elasticity : float
        Weight sensitivity multiplier for km/L.
    ef_co2e_per_l : float
        TTW emission factor for diesel (kg CO2e / L). Default 2.68.

    Returns
    -------
    Dict[str, Any]
    """
    spec = _resolve_spec(truck_key, truck_spec)

    # Resolve axles if missing or auto
    if spec.get("axles") is None or (truck_key == "auto_by_weight"):
        inferred = int(guess_axles_from_payload(float(spec.get("payload_t", 27.0))))
        spec["axles"] = inferred
        _log.info(
            f"estimate_road_trip: axles resolved to {inferred} via payload={spec.get('payload_t')} t "
            f"(truck_key='{truck_key}')."
        )

    # Compute liters via fuel model
    liters_total, liters_loaded, liters_empty, trips, kmL_loaded, kmL_empty = estimate_leg_liters(
        distance_km=float(distance_km),
        cargo_t=float(cargo_t),
        spec=spec,
        empty_backhaul_share=float(empty_backhaul_share),
        elasticity=float(elasticity),
    )

    # Cost
    if diesel_price_brl_l is None:
        fuel_cost_brl = 0.0
        _log.info("estimate_road_trip: diesel_price_brl_l not provided → fuel_cost_brl=0.0 (skipped pricing).")
    else:
        fuel_cost_brl = float(liters_total) * float(diesel_price_brl_l)

    # Emissions (TTW)
    co2e_total_kg = float(liters_total) * float(ef_co2e_per_l)

    # Summary logs
    _log.debug(
        "estimate_road_trip.inputs: "
        f"distance_km={distance_km:.3f}, cargo_t={cargo_t:.3f}, truck_label='{spec.get('label')}', "
        f"axles={spec.get('axles')}, payload_t={spec.get('payload_t')}, empty_share={empty_backhaul_share:.3f}, "
        f"elasticity={elasticity:.3f}, diesel_price_brl_l={diesel_price_brl_l}, ef_co2e_per_l={ef_co2e_per_l:.3f}"
    )
    _log.info(
        "estimate_road_trip.result: liters_total={:.4f}, fuel_cost_brl={:.2f}, co2e_total_kg={:.3f}, "
        "trips={}, kmL_loaded={:.4f}, kmL_empty={:.4f}".format(
            liters_total, fuel_cost_brl, co2e_total_kg, trips, kmL_loaded, kmL_empty
        )
    )

    return {
        "fuel": {
            "liters_total": float(liters_total),
            "liters_loaded": float(liters_loaded),
            "liters_empty": float(liters_empty),
        },
        "cost": {
            "fuel_cost_brl": float(fuel_cost_brl),
            "diesel_price_brl_l": None if diesel_price_brl_l is None else float(diesel_price_brl_l),
        },
        "emissions": {
            "co2e_total_kg": float(co2e_total_kg),
            "ef_diesel_ttw_co2e_kg_per_l": float(ef_co2e_per_l),
        },
        "meta": {
            "truck_label": spec.get("label"),
            "axles": int(spec["axles"]),
            "payload_t": float(spec.get("payload_t", 27.0)),
            "ref_weight_t": float(spec.get("ref_weight_t", 20.0)),
            "empty_efficiency_gain": float(spec.get("empty_efficiency_gain", 0.18)),
            "trips": int(trips),
            "kmL_loaded": float(kmL_loaded),
            "kmL_empty": float(kmL_empty),
            "kmL_baseline": float(get_km_l_baseline(int(spec["axles"]))),
        },
    }


"""
Quick logging smoke test (PowerShell)
python -c `
"from modules.functions.logging import init_logging; `
from modules.road.emissions import estimate_road_trip; `
init_logging(level='INFO', force=True, write_output=False); `
out = estimate_road_trip(distance_km=120.0, cargo_t=40.0, `
    truck_key='auto_by_weight', truck_spec={'payload_t':27.0,'label':'auto payload 27t'}, `
    empty_backhaul_share=0.5, diesel_price_brl_l=6.50); `
print(out); "
"""
