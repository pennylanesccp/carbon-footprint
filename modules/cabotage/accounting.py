# accounting.py
# Purpose: compute fuel consumption, fuel cost, and emissions for multi-stop cabotage,
#          allocating fairly to shipments by distance × weight (tonne-km).
# Style:   commas at the beginning of the line; 4 spaces; verbose comments.
# Units:   distance_km [km], weight_t [tonnes], fuel [kg unless noted], prices [currency/tonne fuel]

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import json
from statistics import mean, median

VERSION: str = "1.1.1"

# ────────────────────────────────────────────────────────────────────────────────
# Data models
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class Leg:
    """
    A sailing leg within a cabotage loop (port i → port i+1).
    Fields:
        id           : human-readable label for the leg (e.g., "Santos→Suape")
        distance_km  : nautical route distance in kilometers
    """
    id: str
    distance_km: float


@dataclass
class Shipment:
    """
    A shipment that rides a subset of legs.
    Fields:
        id        : shipment identifier
        weight_t  : shipment weight in metric tonnes
        on_legs   : list of integer leg indices this shipment is onboard (0-based)
    """
    id: str
    weight_t: float
    on_legs: List[int]

# ────────────────────────────────────────────────────────────────────────────────
# Validation helpers
# ────────────────────────────────────────────────────────────────────────────────

def _validate_inputs(
      legs: List[Leg]
    , shipments: List[Shipment]
) -> None:
    """
    Basic validation to catch common mistakes early:
      • distances must be ≥ 0
      • weights must be ≥ 0
      • on_legs indices must exist
    """
    n_legs = len(legs)
    if n_legs == 0:
        raise ValueError("No legs provided (len(legs)==0).")

    for i, leg in enumerate(legs):
        if leg.distance_km < 0:
            raise ValueError(f"Leg {i} '{leg.id}' has negative distance_km={leg.distance_km}.")

    for s in shipments:
        if s.weight_t < 0:
            raise ValueError(f"Shipment '{s.id}' has negative weight_t={s.weight_t}.")
        for li in s.on_legs:
            if not (0 <= li < n_legs):
                raise IndexError(f"Shipment '{s.id}' refers to leg index {li} not in [0,{n_legs-1}].")

# ────────────────────────────────────────────────────────────────────────────────
# Core tonne-km calculus
# ────────────────────────────────────────────────────────────────────────────────

def compute_tonne_km(
      legs: List[Leg]
    , shipments: List[Shipment]
) -> Tuple[float, Dict[str, float], Dict[int, float], Dict[int, float]]:
    """
    Compute tonne-km totals for a multi-stop loop.

    Returns:
        total_tkm         : float                                – Σ over shipments (Σ over legs in on_legs of weight_t * distance_km)
        tkm_by_shipment   : {shipment_id: tonne_km}
        load_t_by_leg     : {leg_idx: onboard_tonnes on that leg}
        tkm_by_leg        : {leg_idx: leg_tkm = load_t_by_leg[leg]*leg.distance_km}

    Notes:
        • If a shipment appears multiple times on the same leg, that weight is counted once (by construction).
        • A leg with zero onboard tonnes yields leg_tkm=0.
    """
    _validate_inputs(legs=legs, shipments=shipments)

    load_t_by_leg: Dict[int, float] = {idx: 0.0 for idx, _ in enumerate(legs)}
    tkm_by_shipment: Dict[str, float] = {}

    for s in shipments:
        tkm_s = 0.0
        for li in s.on_legs:
            dist = legs[li].distance_km
            tkm_s += s.weight_t * dist
            load_t_by_leg[li] += s.weight_t
        tkm_by_shipment[s.id] = tkm_s

    tkm_by_leg: Dict[int, float] = {
          li: load_t_by_leg[li] * legs[li].distance_km
        for li in range(len(legs))
    }

    total_tkm = sum(tkm_by_shipment.values())
    return total_tkm, tkm_by_shipment, load_t_by_leg, tkm_by_leg

# ────────────────────────────────────────────────────────────────────────────────
# K repository (k.json) – load & select (mean/median/trimmed or by id)
# ────────────────────────────────────────────────────────────────────────────────

def load_k_entries(
      path: str = "k.json"
) -> dict:
    """
    Load K entries from a JSON file with structure:
      {
        "unit": "kg_fuel_per_tonne_km",
        "scope": "TTW",
        "entries": [{"id": "...", "K_kg_per_tkm": 0.0027, "source": "...", ...}]
      }
    Raises:
        ValueError if unit is missing or different from 'kg_fuel_per_tonne_km'
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if data.get("unit") != "kg_fuel_per_tonne_km":
        raise ValueError("k.json unit must be 'kg_fuel_per_tonne_km'.")
    return data


def summarize_Ks(
      entries: List[dict]
    , exclude_ids: Optional[List[str]] = None
) -> dict:
    """
    Return summary stats across Ks.
    Output:
        {'mean': ..., 'median': ..., 'trimmed': ..., 'count': n}
    Trimmed: drop min & max when n >= 3 (robust to outliers).
    """
    ks = [
          float(e["K_kg_per_tkm"])
        for e in entries
        if not (exclude_ids and e.get("id") in exclude_ids)
    ]
    if not ks:
        raise ValueError("No K values available after filtering.")
    ks_sorted = sorted(ks)
    trimmed = mean(ks_sorted[1:-1]) if len(ks_sorted) >= 3 else mean(ks_sorted)
    return {
          "mean": mean(ks)
        , "median": median(ks)
        , "trimmed": trimmed
        , "count": len(ks)
    }


def choose_K(
      data: dict
    , mode: str = "trimmed"    # 'mean' | 'median' | 'trimmed' | 'by_id'
    , by_id: Optional[str] = None
) -> float:
    """
    Pick a single K to use for allocation.
      - mode in {'mean','median','trimmed'} → aggregate over all entries
      - mode == 'by_id' and by_id set      → pick a specific entry (e.g., lane-specific)
    """
    entries = data["entries"]
    if mode == "by_id":
        if not by_id:
            raise ValueError("by_id must be provided when mode='by_id'.")
        for e in entries:
            if e.get("id") == by_id:
                return float(e["K_kg_per_tkm"])
        raise KeyError(f"K id not found: {by_id}")
    if mode not in {"mean", "median", "trimmed"}:
        raise ValueError("mode must be one of {'mean','median','trimmed','by_id'}.")
    stats = summarize_Ks(entries=entries)
    return float(stats[mode])

# ────────────────────────────────────────────────────────────────────────────────
# K – intensity in kg fuel / (tonne-km)
# ────────────────────────────────────────────────────────────────────────────────

def calibrate_K_from_observation(
      observed_fuel_kg: float
    , legs: List[Leg]
    , shipments: List[Shipment]
) -> float:
    """
    Estimate a route-specific K from one observed voyage:
        K = observed_fuel_kg / total_tonne_km
    Caveat:
        Only meaningful if your shipment set spans *all* cargo actually onboard on each leg.
    """
    total_tkm, _, _, _ = compute_tonne_km(legs=legs, shipments=shipments)
    if total_tkm <= 0:
        raise ValueError("Total tonne-km is zero; cannot calibrate K.")
    return observed_fuel_kg / total_tkm


def predict_fuel_from_K(
      K_kg_per_tkm: float
    , legs: List[Leg]
    , shipments: List[Shipment]
) -> Tuple[float, Dict[str, float]]:
    """
    Predict voyage total fuel (kg) and allocate to shipments by their tonne-km share.
    Returns:
        fuel_total_kg, fuel_by_shipment_kg
    """
    total_tkm, tkm_by_shipment, _, _ = compute_tonne_km(legs=legs, shipments=shipments)
    fuel_total_kg = K_kg_per_tkm * total_tkm
    if total_tkm == 0:
        fuel_by_shipment = {sid: 0.0 for sid in tkm_by_shipment}
    else:
        fuel_by_shipment = {
              sid: K_kg_per_tkm * tkm
            for sid, tkm in tkm_by_shipment.items()
        }
    return fuel_total_kg, fuel_by_shipment

# ────────────────────────────────────────────────────────────────────────────────
# Per-leg allocation (lets you apply leg-specific prices or fuel types later)
# ────────────────────────────────────────────────────────────────────────────────

def allocate_fuel_by_leg_and_shipment(
      K_kg_per_tkm: float
    , legs: List[Leg]
    , shipments: List[Shipment]
) -> Tuple[Dict[int, float], Dict[str, float], float]:
    """
    Compute fuel per leg and allocate to shipments.

    Returns:
        fuel_by_leg_kg       : {leg_idx: kg fuel}
        fuel_by_shipment_kg  : {shipment_id: kg fuel}
        fuel_total_kg        : float

    Method:
        • Leg fuel = K × (sum over shipments on that leg of weight_t × distance_leg)
        • Within each leg, split fuel to shipments proportional to their weight on that leg.
    """
    total_tkm, _, load_t_by_leg, tkm_by_leg = compute_tonne_km(legs=legs, shipments=shipments)

    fuel_by_leg_kg: Dict[int, float] = {}
    fuel_by_shipment_kg: Dict[str, float] = {s.id: 0.0 for s in shipments}
    fuel_total_kg = 0.0

    for li, leg in enumerate(legs):
        if load_t_by_leg[li] <= 0 or tkm_by_leg[li] <= 0:
            fuel_by_leg_kg[li] = 0.0
            continue

        f_leg = K_kg_per_tkm * tkm_by_leg[li]
        fuel_by_leg_kg[li] = f_leg
        fuel_total_kg += f_leg

        # split by mass share on the leg
        for s in shipments:
            if li in s.on_legs and s.weight_t > 0:
                share = s.weight_t / load_t_by_leg[li]
                fuel_by_shipment_kg[s.id] += f_leg * share

    _ = total_tkm  # reserved for potential logging
    return fuel_by_leg_kg, fuel_by_shipment_kg, fuel_total_kg

# ────────────────────────────────────────────────────────────────────────────────
# Cost model (kept separate)
# ────────────────────────────────────────────────────────────────────────────────

def fuel_cost(
      fuel_tonnes: float
    , price_per_tonne: float
) -> float:
    """
    Simple multiplication for voyage-level cost.
    """
    return fuel_tonnes * price_per_tonne


def fuel_cost_by_leg(
      fuel_by_leg_kg: Dict[int, float]
    , price_per_tonne_by_leg: Dict[int, float]
) -> Tuple[Dict[int, float], float]:
    """
    Per-leg fuel cost with leg-specific prices.
    Returns:
        cost_by_leg: {leg_idx: cost}
        total_cost : float
    """
    cost_by_leg: Dict[int, float] = {}
    total_cost = 0.0
    for li, f_kg in fuel_by_leg_kg.items():
        f_t = f_kg / 1000.0
        price = price_per_tonne_by_leg.get(li, 0.0)
        c = f_t * price
        cost_by_leg[li] = c
        total_cost += c
    return cost_by_leg, total_cost

# ────────────────────────────────────────────────────────────────────────────────
# Emissions – TtW with flexible factors (kept separate)
# ────────────────────────────────────────────────────────────────────────────────

def emissions_ttw(
      fuel_kg: float
    , ef_ttw_per_tonne_fuel: Dict[str, float]
    , gwp100: Optional[Dict[str, float]] = None
) -> Dict[str, float]:
    """
    Compute mass of each gas and total CO2e (TtW).
    Args:
        fuel_kg: fuel mass (kg)
        ef_ttw_per_tonne_fuel: dict with keys like 'CO2', 'CH4', 'N2O' giving kg gas / tonne fuel
                               (provide per chosen fuel type, e.g., MGO or HFO)
        gwp100: optional dict {'CH4': 29.8, 'N2O': 273, ...} for CO2e
    Returns:
        {'CO2': kg, 'CH4': kg, 'N2O': kg, 'CO2e': kg}
    """
    fuel_t = fuel_kg / 1000.0
    out: Dict[str, float] = {}
    co2e = 0.0
    for gas, ef_per_t in ef_ttw_per_tonne_fuel.items():
        mass = ef_per_t * fuel_t
        out[gas] = mass
        if gas.upper() == "CO2":
            co2e += mass
    if gwp100:
        for gas, mass in out.items():
            if gas.upper() != "CO2" and gas in gwp100:
                co2e += mass * gwp100[gas]
    out["CO2e"] = co2e
    return out


def emissions_ttw_by_leg(
      fuel_by_leg_kg: Dict[int, float]
    , ef_ttw_per_tonne_fuel_by_leg: Dict[int, Dict[str, float]]
    , gwp100: Optional[Dict[str, float]] = None
) -> Tuple[Dict[int, Dict[str, float]], Dict[str, float]]:
    """
    Compute TtW emissions per leg with leg-specific emission factors (fuel type swaps etc.).
    Returns:
        emis_by_leg: {leg_idx: {'CO2': ..., 'CH4': ..., 'N2O': ..., 'CO2e': ...}}
        totals     : aggregated {'CO2': ..., 'CH4': ..., 'N2O': ..., 'CO2e': ...}
    """
    emis_by_leg: Dict[int, Dict[str, float]] = {}
    totals: Dict[str, float] = {"CO2": 0.0, "CH4": 0.0, "N2O": 0.0, "CO2e": 0.0}

    for li, f_kg in fuel_by_leg_kg.items():
        ef = ef_ttw_per_tonne_fuel_by_leg.get(li, {})
        res = emissions_ttw(
              fuel_kg=f_kg
            , ef_ttw_per_tonne_fuel=ef
            , gwp100=gwp100
        )
        emis_by_leg[li] = res
        for k in totals:
            totals[k] += res.get(k, 0.0)

    return emis_by_leg, totals

# ────────────────────────────────────────────────────────────────────────────────
# Convenience: allocate cost & emissions to shipments from fuel allocation
# ────────────────────────────────────────────────────────────────────────────────

def allocate_costs_emissions(
      fuel_by_shipment_kg: Dict[str, float]
    , price_per_tonne: float
    , ef_ttw_per_tonne_fuel: Dict[str, float]
    , gwp100: Optional[Dict[str, float]] = None
) -> Dict[str, Dict[str, float]]:
    """
    Build a per-shipment report joining fuel, cost and TtW emissions.
    Returns:
        {
          shipment_id: {
              'fuel_kg': ...
            , 'fuel_t': ...
            , 'cost': ...
            , 'CO2': ...
            , 'CH4': ...
            , 'N2O': ...
            , 'CO2e': ...
          }, ...
        }
    """
    results: Dict[str, Dict[str, float]] = {}
    for sid, f_kg in fuel_by_shipment_kg.items():
        f_t = f_kg / 1000.0
        cost_val = fuel_cost(
              fuel_tonnes=f_t
            , price_per_tonne=price_per_tonne
        )
        emis = emissions_ttw(
              fuel_kg=f_kg
            , ef_ttw_per_tonne_fuel=ef_ttw_per_tonne_fuel
            , gwp100=gwp100
        )
        results[sid] = {
              "fuel_kg": f_kg
            , "fuel_t": f_t
            , "cost": cost_val
            , **emis
        }
    return results

# ────────────────────────────────────────────────────────────────────────────────
# Example usage (can be run as a script)
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Example legs and shipments
    legs = [
          Leg(id="Santos→Suape", distance_km=2000.0)
        , Leg(id="Suape→Fortaleza", distance_km=800.0)
    ]
    shipments = [
          Shipment(id="S1", weight_t=20.0, on_legs=[0, 1])   # stays on board both legs
        , Shipment(id="S2", weight_t=10.0, on_legs=[0])      # discharges at Suape
    ]

    # Load K from k.json (use trimmed mean for robustness)
    repo = load_k_entries(path="k.json")
    K = choose_K(repo, mode="trimmed")   # positional 'repo' is OK
    print(f"[accounting.py v{VERSION}] Using K = {K:.6f} kg fuel / t·km")

    # Allocate fuel per leg, then to shipments (lets you price per leg later if needed)
    fuel_by_leg_kg, fuel_by_ship_kg, fuel_total_kg = allocate_fuel_by_leg_and_shipment(
          K_kg_per_tkm=K
        , legs=legs
        , shipments=shipments
    )
    print("Fuel by leg (kg):", fuel_by_leg_kg)
    print("Fuel by shipment (kg):", fuel_by_ship_kg)
    print("Fuel total (kg):", fuel_total_kg)

    # Cost (single price for the whole voyage in this example)
    price_per_tonne_brl = 3_200.0  # example; replace with Ship & Bunker series if desired

    # Emission factors (TtW) – example values; replace with your chosen source values
    ef_ttw_mgo = {
          "CO2": 3206.0
        , "CH4": 0.0
        , "N2O": 0.0
    }
    gwp100_ar6 = {
          "CH4": 29.8
        , "N2O": 273.0
    }

    # Per-shipment final report (fuel, cost, TtW emissions)
    report = allocate_costs_emissions(
          fuel_by_shipment_kg=fuel_by_ship_kg
        , price_per_tonne=price_per_tonne_brl
        , ef_ttw_per_tonne_fuel=ef_ttw_mgo
        , gwp100=gwp100_ar6
    )
    print("Per-shipment report:", report)


# ────────────────────────────────────────────────────────────────────────────────
# Port handling – single constant K_port (kg fuel per tonne handled)
# ────────────────────────────────────────────────────────────────────────────────

def port_fuel_from_handled_mass(
      handled_mass_t: float
    , K_port_kg_per_t: float = 0.48
) -> float:
    """
    Fuel used in a port call for loading/unloading.
    Args:
        handled_mass_t   : total tonnes actually moved ship↔yard in this call
        K_port_kg_per_t  : kg fuel per tonne handled (default ≈ 0.48 kg/t)
    Returns:
        fuel_kg          : kg fuel for this port call
    """
    if handled_mass_t < 0:
        raise ValueError("handled_mass_t must be >= 0.")
    if K_port_kg_per_t < 0:
        raise ValueError("K_port_kg_per_t must be >= 0.")
    return handled_mass_t * K_port_kg_per_t


def allocate_port_fuel_to_shipments(
      handled_mass_by_shipment_t: Dict[str, float]
    , K_port_kg_per_t: float = 0.48
) -> Dict[str, float]:
    """
    Allocate port fuel directly to the shipments that were handled (loaded/discharged).
    Args:
        handled_mass_by_shipment_t : {shipment_id: tonnes moved at this port}
        K_port_kg_per_t            : kg fuel per tonne handled
    Returns:
        fuel_by_shipment_kg        : {shipment_id: kg fuel attributed at this port}
    """
    out: Dict[str, float] = {}
    for sid, m_t in handled_mass_by_shipment_t.items():
        if m_t < 0:
            raise ValueError(f"Negative handled mass for shipment {sid}: {m_t}")
        out[sid] = m_t * K_port_kg_per_t
    return out
