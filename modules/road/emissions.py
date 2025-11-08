# -*- coding: utf-8 -*-
# Road cargo fuel, emissions, and cost estimators
# Method: fuel-based (preferred by IPCC/ISO 14083/GLEC)
# Notes:
#   • CO₂ from diesel combustion dominates; CH₄/N₂O tailpipe are small but supported.
#   • Defaults below are conservative placeholders — calibrate km/L by truck class + route.
#   • Diesel CO₂ factor derived from IPCC defaults (see project docs/citations).

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any

# ─────────────────────────────── constants (override via args if needed)
# CO₂ per liter diesel (tailpipe only). Derivable from IPCC 2006:
#   74,100 kg CO₂/TJ (gas/diesel oil) × NCV × density ⇒ ≈ 2.68 kg CO₂/L.
EF_DIESEL_CO2_KG_PER_L: float = 2.68

# IPCC default tailpipe factors for heavy-duty diesel (order-of-magnitude; per km)
# (Values are small vs CO₂; include for completeness. Disable via include_ch4n2o=False.)
EF_CH4_MG_PER_KM_HDV: float = 30.0   # mg CH₄ / km
EF_N2O_MG_PER_KM_HDV: float = 30.0   # mg N₂O / km

# 100-yr Global Warming Potentials (IPCC AR5 commonly used by inventories)
GWP_CH4: float = 28.0
GWP_N2O: float = 265.0

# ─────────────────────────────── truck spec
@dataclass(frozen=True)
class TruckSpec:
    name: str
    payload_t: float             # typical payload capacity (t)
    km_per_l_loaded: float       # fuel economy when loaded (km/L)
    km_per_l_empty: float        # fuel economy when empty (km/L)

# Example presets — tune to your fleet/telematics later
TRUCK_SPECS: Dict[str, TruckSpec] = {
      "rigid_14t": TruckSpec(
            name="Rigid 14t"
        ,   payload_t=9.0
        ,   km_per_l_loaded=3.5
        ,   km_per_l_empty=4.5
    )
    , "semi_27t": TruckSpec(
            name="Tractor–semitrailer 27t"
        ,   payload_t=27.0
        ,   km_per_l_loaded=2.7
        ,   km_per_l_empty=3.4
    )
    , "bitrem_36t": TruckSpec(
            name="Bi-train 36t"
        ,   payload_t=36.0
        ,   km_per_l_loaded=2.4
        ,   km_per_l_empty=3.0
    )
}

# ─────────────────────────────── core calc
def estimate_road_trip(
      *
    , distance_km: float
    , cargo_t: float
    , diesel_price_brl_per_l: float
    , spec: TruckSpec
    , empty_backhaul_share: float = 0.20          # e.g., 20% of distance is empty reposition
    , ef_co2_kg_per_l: float = EF_DIESEL_CO2_KG_PER_L
    , ef_ch4_mg_per_km: float = EF_CH4_MG_PER_KM_HDV
    , ef_n2o_mg_per_km: float = EF_N2O_MG_PER_KM_HDV
    , gwp_ch4: float = GWP_CH4
    , gwp_n2o: float = GWP_N2O
    , include_ch4n2o: bool = True
) -> Dict[str, Any]:
    """
    Returns totals and per-kg metrics for a single lane movement.

    distance_km            – one-way distance carrying cargo
    cargo_t                – delivered cargo mass (tonnes) on that movement
    diesel_price_brl_per_l – average diesel price used in this estimate
    spec                   – TruckSpec with km/L (loaded & empty)
    empty_backhaul_share   – fraction of distance run empty (0..1)

    All CH₄/N₂O are tailpipe only; CO₂ is tailpipe only (no well-to-tank by default).
    """
    # Defensive guards
    if distance_km <= 0.0:
        raise ValueError("distance_km must be > 0")
    if cargo_t <= 0.0:
        raise ValueError("cargo_t must be > 0")
    if spec.km_per_l_loaded <= 0.0 or spec.km_per_l_empty <= 0.0:
        raise ValueError("km/L must be > 0")

    # Fuel use (L)
    liters_loaded = distance_km / spec.km_per_l_loaded
    liters_empty  = (distance_km * float(empty_backhaul_share)) / spec.km_per_l_empty
    liters_total  = liters_loaded + liters_empty

    # CO₂ (kg) — fuel-based
    co2_kg = liters_total * float(ef_co2_kg_per_l)

    # CH₄ + N₂O (kg CO₂e) — distance-based tailpipe add-ons
    # Apply to total driven km (loaded + empty)
    total_km_run = distance_km * (1.0 + float(empty_backhaul_share))
    ch4_co2e_kg = 0.0
    n2o_co2e_kg = 0.0
    if include_ch4n2o:
        ch4_co2e_kg = (ef_ch4_mg_per_km / 1e6) * total_km_run * gwp_ch4
        n2o_co2e_kg = (ef_n2o_mg_per_km / 1e6) * total_km_run * gwp_n2o

    co2e_total_kg = co2_kg + ch4_co2e_kg + n2o_co2e_kg

    # Cost (BRL)
    fuel_cost_brl = liters_total * float(diesel_price_brl_per_l)

    # Normalisations
    cargo_kg = cargo_t * 1000.0
    per_kg = {
          "fuel_l_per_kg":       liters_total / cargo_kg
        , "co2e_kg_per_kg":      co2e_total_kg / cargo_kg
        , "co2_kg_per_kg":       co2_kg / cargo_kg
        , "cost_brl_per_kg":     fuel_cost_brl / cargo_kg
        , "gco2e_per_tkm": (co2e_total_kg / (cargo_t * distance_km)) * 1e3  # g CO₂e / t·km
        , "kgco2e_per_tkm": (co2e_total_kg / (cargo_t * distance_km))
    }

    return {
          "inputs": {
                "distance_km": distance_km
            ,   "cargo_t": cargo_t
            ,   "diesel_price_brl_per_l": diesel_price_brl_per_l
            ,   "spec": spec.__dict__
            ,   "empty_backhaul_share": float(empty_backhaul_share)
            ,   "factors": {
                      "ef_co2_kg_per_l": ef_co2_kg_per_l
                    , "ef_ch4_mg_per_km": ef_ch4_mg_per_km
                    , "ef_n2o_mg_per_km": ef_n2o_mg_per_km
                    , "gwp_ch4": gwp_ch4
                    , "gwp_n2o": gwp_n2o
                }
        }
        , "fuel": {
                "liters_loaded": liters_loaded
            ,   "liters_empty": liters_empty
            ,   "liters_total": liters_total
        }
        , "emissions": {
                "co2_kg": co2_kg
            ,   "ch4_co2e_kg": ch4_co2e_kg
            ,   "n2o_co2e_kg": n2o_co2e_kg
            ,   "co2e_total_kg": co2e_total_kg
        }
        , "cost": {
                "fuel_cost_brl": fuel_cost_brl
        }
        , "per_kg": per_kg
    }
