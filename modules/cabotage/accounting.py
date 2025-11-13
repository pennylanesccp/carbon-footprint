# modules/cabotage/accounting.py
# -*- coding: utf-8 -*-
"""
Cabotage accounting
===================

Purpose
-------
Compute **fuel consumption**, **fuel cost**, and **TTW emissions** for multi-stop cabotage,
allocating fairly to shipments by **tonne-km** (distance × weight). Also provides:
- Voyage K selection (kg fuel / tonne-km) from a repository (k.json),
- Per-leg allocation (for leg-varying prices or fuel types),
- Port handling fuel allocation (load/discharge),
- Hotel-at-berth allocation from a per-city index (hotel.json).

Units
-----
- distance_km: km
- weight_t   : tonnes (t)
- fuel       : kg (unless stated otherwise)
- prices     : currency / tonne fuel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from statistics import mean, median
import json
import os
import re

from modules.functions._logging import get_logger

_log = get_logger(__name__)

VERSION: str = "1.2.0"

# ────────────────────────────────────────────────────────────────────────────────
# Data models
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class Leg:
    """
    Sailing leg within a cabotage loop (port i → port i+1).

    Attributes
    ----------
    id : str
        Human-readable label for the leg (e.g., "Santos→Suape").
    distance_km : float
        Nautical route distance in kilometers (precomputed/curated).
    """
    id: str
    distance_km: float


@dataclass
class Shipment:
    """
    Shipment that rides a subset of legs.

    Attributes
    ----------
    id : str
        Shipment identifier.
    weight_t : float
        Shipment weight in metric tonnes.
    on_legs : List[int]
        List of integer leg indices (0-based) this shipment is onboard.
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

    _log.debug("Input validation OK: legs=%d, shipments=%d.", len(legs), len(shipments))


# ────────────────────────────────────────────────────────────────────────────────
# Core tonne-km calculus
# ────────────────────────────────────────────────────────────────────────────────

def compute_tonne_km(
      legs: List[Leg]
    , shipments: List[Shipment]
) -> Tuple[float, Dict[str, float], Dict[int, float], Dict[int, float]]:
    """
    Compute tonne-km totals for a multi-stop loop.

    Returns
    -------
    total_tkm : float
        Σ shipments( Σ legs∈on_legs (weight_t × distance_km) )
    tkm_by_shipment : Dict[str, float]
    load_t_by_leg   : Dict[int, float]
        Onboard tonnes per leg.
    tkm_by_leg      : Dict[int, float]
        Leg tonne-km = load_t_by_leg[leg] × leg.distance_km

    Notes
    -----
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

    _log.info(
        "compute_tonne_km: total_tkm=%.6f, legs=%d, shipments=%d.",
        total_tkm, len(legs), len(shipments)
    )
    _log.debug("compute_tonne_km: tkm_by_shipment=%r", tkm_by_shipment)
    _log.debug("compute_tonne_km: load_t_by_leg=%r", load_t_by_leg)
    _log.debug("compute_tonne_km: tkm_by_leg=%r", tkm_by_leg)

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

    Raises
    ------
    ValueError
        If unit is missing or different from 'kg_fuel_per_tonne_km'
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    unit = data.get("unit")
    if unit != "kg_fuel_per_tonne_km":
        raise ValueError("k.json unit must be 'kg_fuel_per_tonne_km'.")
    _log.info("load_k_entries: loaded %d entries from '%s' (unit=%s).", len(data.get("entries", [])), path, unit)
    return data


def summarize_Ks(
      entries: List[dict]
    , exclude_ids: Optional[List[str]] = None
) -> dict:
    """
    Return summary stats across Ks.

    Output
    ------
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

    out = {
          "mean": mean(ks)
        , "median": median(ks)
        , "trimmed": trimmed
        , "count": len(ks)
    }
    _log.info("summarize_Ks: count=%d mean=%.6f median=%.6f trimmed=%.6f.", out["count"], out["mean"], out["median"], out["trimmed"])
    return out


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
    entries = data.get("entries") or []
    if mode == "by_id":
        if not by_id:
            raise ValueError("by_id must be provided when mode='by_id'.")
        for e in entries:
            if e.get("id") == by_id:
                val = float(e["K_kg_per_tkm"])
                _log.info("choose_K(by_id): id='%s' K=%.6f.", by_id, val)
                return val
        raise KeyError(f"K id not found: {by_id}")

    if mode not in {"mean", "median", "trimmed"}:
        raise ValueError("mode must be one of {'mean','median','trimmed','by_id'}.")

    stats = summarize_Ks(entries=entries)
    val = float(stats[mode])
    _log.info("choose_K(%s): K=%.6f.", mode, val)
    return val


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
    k = observed_fuel_kg / total_tkm
    _log.info("calibrate_K_from_observation: observed_fuel_kg=%.3f total_tkm=%.6f → K=%.9f.", observed_fuel_kg, total_tkm, k)
    return k


def predict_fuel_from_K(
      K_kg_per_tkm: float
    , legs: List[Leg]
    , shipments: List[Shipment]
) -> Tuple[float, Dict[str, float]]:
    """
    Predict voyage total fuel (kg) and allocate to shipments by their tonne-km share.

    Returns
    -------
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

    _log.info("predict_fuel_from_K: K=%.6f total_tkm=%.6f → fuel_total_kg=%.3f.", K_kg_per_tkm, total_tkm, fuel_total_kg)
    _log.debug("predict_fuel_from_K: fuel_by_shipment=%r", fuel_by_shipment)
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

    Returns
    -------
    fuel_by_leg_kg       : {leg_idx: kg fuel}
    fuel_by_shipment_kg  : {shipment_id: kg fuel}
    fuel_total_kg        : float

    Method
    ------
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

    _ = total_tkm  # retained for potential future logs
    _log.info(
        "allocate_fuel_by_leg_and_shipment: legs=%d shipments=%d fuel_total_kg=%.3f.",
        len(legs), len(shipments), fuel_total_kg
    )
    _log.debug("allocate_fuel_by_leg_and_shipment: fuel_by_leg_kg=%r", fuel_by_leg_kg)
    _log.debug("allocate_fuel_by_leg_and_shipment: fuel_by_shipment_kg=%r", fuel_by_shipment_kg)

    return fuel_by_leg_kg, fuel_by_shipment_kg, fuel_total_kg


# ────────────────────────────────────────────────────────────────────────────────
# Cost model (kept separate)
# ────────────────────────────────────────────────────────────────────────────────

def fuel_cost(
      fuel_tonnes: float
    , price_per_tonne: float
) -> float:
    """Simple multiplication for voyage-level cost."""
    c = fuel_tonnes * price_per_tonne
    _log.debug("fuel_cost: fuel_t=%.6f price=%.2f → cost=%.2f.", fuel_tonnes, price_per_tonne, c)
    return c


def fuel_cost_by_leg(
      fuel_by_leg_kg: Dict[int, float]
    , price_per_tonne_by_leg: Dict[int, float]
) -> Tuple[Dict[int, float], float]:
    """
    Per-leg fuel cost with leg-specific prices.

    Returns
    -------
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

    _log.info("fuel_cost_by_leg: legs=%d total_cost=%.2f.", len(fuel_by_leg_kg), total_cost)
    _log.debug("fuel_cost_by_leg: cost_by_leg=%r", cost_by_leg)
    return cost_by_leg, total_cost


# ────────────────────────────────────────────────────────────────────────────────
# Emissions – TtW with flexible factors (kept separate)
# ────────────────────────────────────────────────────────────────────────────────

def emissions_ttw(*, fuel_kg: float, ef_ttw_per_tonne_fuel: Dict[str, float], gwp100: Dict[str, float]) -> Dict[str, float]:
    """
    Tailpipe (TTW) emissions from fuel mass.

    Parameters
    ----------
    fuel_kg : float
    ef_ttw_per_tonne_fuel : Dict[str, float]
        {'CO2': kg/t-fuel, 'CH4': kg/t-fuel, 'N2O': kg/t-fuel}
    gwp100 : Dict[str, float]
        {'CH4': ..., 'N2O': ...}

    Returns
    -------
    {'CO2': ..., 'CH4': ..., 'N2O': 0.0, 'CO2e': ...}
    """
    t_fuel = float(fuel_kg) / 1000.0
    co2 = t_fuel * float(ef_ttw_per_tonne_fuel.get("CO2", 0.0))
    ch4 = t_fuel * float(ef_ttw_per_tonne_fuel.get("CH4", 0.0))
    n2o = t_fuel * 0.0  # placeholder for future factors
    co2e = co2 + ch4 * float(gwp100.get("CH4", 0.0)) + n2o * float(gwp100.get("N2O", 0.0))
    out = {
          "CO2": co2
        , "CH4": ch4
        , "N2O": n2o
        , "CO2e": co2e
    }
    _log.debug("emissions_ttw: fuel_kg=%.3f → %r", fuel_kg, out)
    return out


def emissions_ttw_by_leg(
      fuel_by_leg_kg: Dict[int, float]
    , ef_ttw_per_tonne_fuel_by_leg: Dict[int, Dict[str, float]]
    , gwp100: Optional[Dict[str, float]] = None
) -> Tuple[Dict[int, Dict[str, float]], Dict[str, float]]:
    """
    Compute TtW emissions per leg with leg-specific emission factors (fuel type swaps etc.).

    Returns
    -------
    emis_by_leg: {leg_idx: {'CO2': ..., 'CH4': ..., 'N2O': ..., 'CO2e': ...}}
    totals     : aggregated {'CO2': ..., 'CH4': ..., 'N2O': ..., 'CO2e': ...}
    """
    gwp = gwp100 or {"CH4": 0.0, "N2O": 0.0}
    emis_by_leg: Dict[int, Dict[str, float]] = {}
    totals: Dict[str, float] = {"CO2": 0.0, "CH4": 0.0, "N2O": 0.0, "CO2e": 0.0}

    for li, f_kg in fuel_by_leg_kg.items():
        ef = ef_ttw_per_tonne_fuel_by_leg.get(li, {})
        res = emissions_ttw(
              fuel_kg=f_kg
            , ef_ttw_per_tonne_fuel=ef
            , gwp100=gwp
        )
        emis_by_leg[li] = res
        for k in totals:
            totals[k] += res.get(k, 0.0)

    _log.info("emissions_ttw_by_leg: legs=%d CO2e_total=%.3f.", len(emis_by_leg), totals["CO2e"])
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

    Returns
    -------
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
    gwp = gwp100 or {"CH4": 0.0, "N2O": 0.0}

    for sid, f_kg in fuel_by_shipment_kg.items():
        f_t = f_kg / 1000.0
        cost_val = fuel_cost(
              fuel_tonnes=f_t
            , price_per_tonne=price_per_tonne
        )
        emis = emissions_ttw(
              fuel_kg=f_kg
            , ef_ttw_per_tonne_fuel=ef_ttw_per_tonne_fuel
            , gwp100=gwp
        )
        results[sid] = {
              "fuel_kg": f_kg
            , "fuel_t": f_t
            , "cost": cost_val
            , **emis
        }

    _log.info("allocate_costs_emissions: shipments=%d.", len(results))
    return results


# ────────────────────────────────────────────────────────────────────────────────
# Port handling – single constant K_port (kg fuel per tonne handled)
# ────────────────────────────────────────────────────────────────────────────────

def port_fuel_from_handled_mass(
      handled_mass_t: float
    , K_port_kg_per_t: float = 0.48
) -> float:
    """
    Fuel used in a port call for loading/unloading.

    Parameters
    ----------
    handled_mass_t : float
        Total tonnes actually moved ship↔yard in this call.
    K_port_kg_per_t : float, default 0.48
        kg fuel per tonne handled.

    Returns
    -------
    fuel_kg : float
        kg fuel for this port call.
    """
    if handled_mass_t < 0:
        raise ValueError("handled_mass_t must be >= 0.")
    if K_port_kg_per_t < 0:
        raise ValueError("K_port_kg_per_t must be >= 0.")
    fuel = handled_mass_t * K_port_kg_per_t
    _log.debug("port_fuel_from_handled_mass: handled=%.3f t, K=%.3f → fuel=%.3f kg.", handled_mass_t, K_port_kg_per_t, fuel)
    return fuel


def allocate_port_fuel_to_shipments(
      handled_mass_by_shipment_t: Dict[str, float]
    , K_port_kg_per_t: float = 0.48
) -> Dict[str, float]:
    """
    Allocate port fuel directly to the shipments that were handled (loaded/discharged).

    Parameters
    ----------
    handled_mass_by_shipment_t : Dict[str, float]
        {shipment_id: tonnes moved at this port}
    K_port_kg_per_t : float
        kg fuel per tonne handled.

    Returns
    -------
    fuel_by_shipment_kg : Dict[str, float]
    """
    out: Dict[str, float] = {}
    for sid, m_t in handled_mass_by_shipment_t.items():
        if m_t < 0:
            raise ValueError(f"Negative handled mass for shipment {sid}: {m_t}")
        out[sid] = m_t * K_port_kg_per_t
    _log.debug("allocate_port_fuel_to_shipments: %r", out)
    return out


# ────────────────────────────────────────────────────────────────────────────────
# Hotel @ berth (kg fuel per tonne handled) – from modules/cabotage/_data/hotel.json
# ────────────────────────────────────────────────────────────────────────────────

def load_hotel_entries(
    *
    , path: str = os.path.join("modules", "cabotage", "._data".replace("._", "_"), "hotel.json")
) -> dict:
    """
    Load hotel.json payload produced by calcs/hotel.py.

    Expected shape
    --------------
      {
        "unit": "kg_fuel_per_tonne",
        "scope": "hotel_at_berth",
        "entries": [
          {"city": "Santos", "uf": "São Paulo", "kg_fuel_per_t": 1.261514, ...},
          ...
        ]
      }

    Returns
    -------
    dict
        The parsed payload.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    unit = data.get("unit")
    scope = data.get("scope")
    if unit != "kg_fuel_per_tonne" or scope != "hotel_at_berth":
        raise ValueError("hotel.json must have unit='kg_fuel_per_tonne' and scope='hotel_at_berth'.")
    if not isinstance(data.get("entries"), list):
        raise ValueError("hotel.json missing 'entries' list.")

    _log.info("load_hotel_entries: loaded %d entries from '%s'.", len(data.get("entries", [])), path)
    return data


def _norm_city(s: str) -> str:
    """Very light normalization for city labels (spaces collapse; accents kept)."""
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def build_hotel_factor_index(
    *
    , hotel_data: dict
) -> Dict[str, float]:
    """
    Build a fast {city -> kg_fuel_per_t} index from hotel.json. Ignores entries where value is null.
    """
    idx: Dict[str, float] = {}
    for e in hotel_data.get("entries", []):
        city = _norm_city(e.get("city", ""))
        val = e.get("kg_fuel_per_t")
        if city and isinstance(val, (int, float)):
            idx[city] = float(val)

    if not idx:
        raise ValueError("No usable city factors found in hotel.json entries.")

    _log.info("build_hotel_factor_index: cities=%d.", len(idx))
    return idx


def _split_ports_for_hotel(leg_id: str) -> Tuple[str, str]:
    """
    Split a leg label like 'Santos→Suape' or 'Santos->Suape' into ('Santos','Suape').
    More lenient with separators but keeps labels intact to match hotel.json.
    """
    if "→" in leg_id:
        a, b = leg_id.split("→", 1)
    elif "->" in leg_id:
        a, b = leg_id.split("->", 1)
    else:
        # best-effort: unify various dashes/arrows into '->'
        tmp = leg_id.replace("—", "->").replace("–", "->").replace("-", "->")
        parts = [p.strip() for p in tmp.split("->") if p.strip()]
        if len(parts) != 2:
            raise ValueError(f"Cannot infer origin/destination from leg id: {leg_id}")
        a, b = parts
    return _norm_city(a), _norm_city(b)


def allocate_hotel_fuel_from_json(
    *
    , legs: List[Leg]
    , shipments: List[Shipment]
    , hotel_json_path: str = os.path.join("data", "cabotage_data", "hotel.json")
    , default_kg_per_t: Optional[float] = None   # if a port city is missing, either use this fallback or skip
    , on_missing: str = "skip"                   # 'skip' | 'use_default' | 'raise'
) -> Dict[str, Any]:
    """
    Allocate *hotel-at-berth* fuel to **handled cargo only**, using per-city kg_fuel_per_t
    factors from hotel.json. For each shipment:
      • load at the origin city of its first leg
      • discharge at the destination city of its last leg
    Each handling event adds:  weight_t × kg_fuel_per_t(city)

    Returns
    -------
    {
      'fuel_by_port_by_ship_kg':     {city: {shipment_id: kg, ...}, ...},
      'fuel_by_port_total_kg':       {city: kg_total, ...},
      'fuel_by_shipment_total_kg':   {shipment_id: kg, ...},
      'fuel_total_kg':               kg_sum,
      'missing_cities':              [city, ...]
    }

    Notes
    -----
    • This follows the convention "hotel fuel allocated to handled tonnes",
      *not* to all onboard cargo during berth time.
    • City labels must match hotel.json entries (after light normalization).
    """
    # Build leg origin/destination mapping
    od_by_leg: Dict[int, Tuple[str, str]] = {}
    for i, leg in enumerate(legs):
        o, d = _split_ports_for_hotel(leg.id)
        od_by_leg[i] = (o, d)

    # Load and index per-city factors
    hotel_data = load_hotel_entries(path=hotel_json_path)
    factor_by_city = build_hotel_factor_index(hotel_data=hotel_data)

    fuel_by_port_by_ship_kg: Dict[str, Dict[str, float]] = {}
    fuel_by_port_total_kg: Dict[str, float] = {}
    fuel_by_shipment_total_kg: Dict[str, float] = {}
    missing_cities: List[str] = []

    # Helper to fetch city factor with policy on missing
    def _get_factor(city: str) -> Optional[float]:
        if city in factor_by_city:
            return factor_by_city[city]
        if on_missing == "use_default" and default_kg_per_t is not None:
            return float(default_kg_per_t)
        if on_missing == "raise":
            raise KeyError(f"City '{city}' not found in hotel.json and no default provided.")
        # on_missing == 'skip'
        if city not in missing_cities:
            missing_cities.append(city)
        return None

    # For each shipment, add fuel at load port and discharge port
    for s in shipments:
        if not s.on_legs:
            continue
        first_leg = min(s.on_legs)
        last_leg  = max(s.on_legs)

        load_city, _ = od_by_leg[first_leg]
        _, discharge_city = od_by_leg[last_leg]

        # load event
        k_load = _get_factor(load_city)
        if k_load is not None and s.weight_t > 0:
            fuel = s.weight_t * k_load
            fuel_by_port_by_ship_kg.setdefault(load_city, {})
            fuel_by_port_by_ship_kg[load_city][s.id] = fuel_by_port_by_ship_kg[load_city].get(s.id, 0.0) + fuel
            fuel_by_port_total_kg[load_city] = fuel_by_port_total_kg.get(load_city, 0.0) + fuel
            fuel_by_shipment_total_kg[s.id]  = fuel_by_shipment_total_kg.get(s.id, 0.0) + fuel

        # discharge event
        k_disc = _get_factor(discharge_city)
        if k_disc is not None and s.weight_t > 0:
            fuel = s.weight_t * k_disc
            fuel_by_port_by_ship_kg.setdefault(discharge_city, {})
            fuel_by_port_by_ship_kg[discharge_city][s.id] = fuel_by_port_by_ship_kg[discharge_city].get(s.id, 0.0) + fuel
            fuel_by_port_total_kg[discharge_city] = fuel_by_port_total_kg.get(discharge_city, 0.0) + fuel
            fuel_by_shipment_total_kg[s.id]       = fuel_by_shipment_total_kg.get(s.id, 0.0) + fuel

    fuel_total_kg = sum(fuel_by_port_total_kg.values())

    _log.info(
        "allocate_hotel_fuel_from_json: cities=%d shipments=%d fuel_total_kg=%.3f (missing=%d).",
        len(fuel_by_port_total_kg), len(shipments), fuel_total_kg, len(missing_cities)
    )
    _log.debug("allocate_hotel_fuel_from_json: missing_cities=%r", missing_cities)

    return {
          "fuel_by_port_by_ship_kg": fuel_by_port_by_ship_kg
        , "fuel_by_port_total_kg":   fuel_by_port_total_kg
        , "fuel_by_shipment_total_kg": fuel_by_shipment_total_kg
        , "fuel_total_kg":           fuel_total_kg
        , "missing_cities":          missing_cities
    }


"""
Quick logging smoke test (PowerShell)
python -c `
"from modules.functions.logging import init_logging; `
from modules.cabotage.accounting import (Leg, Shipment, compute_tonne_km, summarize_Ks, choose_K, `
    calibrate_K_from_observation, predict_fuel_from_K, allocate_fuel_by_leg_and_shipment, `
    fuel_cost_by_leg, emissions_ttw_by_leg, allocate_costs_emissions); `
init_logging(level='INFO', force=True, write_output=False); `
legs = [ `
    Leg(id='Santos→Suape', distance_km=2000.0) `
  , Leg(id='Suape→Fortaleza', distance_km=800.0) `
]; `
ships = [ `
    Shipment(id='S1', weight_t=20.0, on_legs=[0,1]) `
  , Shipment(id='S2', weight_t=10.0, on_legs=[0]) `
]; `
total_tkm, tkm_by_ship, load_by_leg, tkm_by_leg = compute_tonne_km(legs, ships); `
print('tkm_total=', round(total_tkm,3)); `
repo = {'unit':'kg_fuel_per_tonne_km','entries':[{'id':'x','K_kg_per_tkm':0.0028}, {'id':'y','K_kg_per_tkm':0.0026}]}; `
print('Ks:', summarize_Ks(repo['entries'])); `
K = choose_K(repo, mode='trimmed'); `
fuel_by_leg, fuel_by_ship, fuel_total = allocate_fuel_by_leg_and_shipment(K, legs, ships); `
print('fuel_total=', round(fuel_total,3)); `
cost_by_leg, cost_total = fuel_cost_by_leg(fuel_by_leg, {0:3200.0, 1:3400.0}); `
print('cost_total=', round(cost_total,2)); `
emis_by_leg, totals = emissions_ttw_by_leg(fuel_by_leg, {0:{'CO2':3206.0}, 1:{'CO2':3206.0}}, {'CH4':29.8,'N2O':273.0}); `
print('CO2e_total=', round(totals['CO2e'],3)); `
report = allocate_costs_emissions(fuel_by_ship, price_per_tonne=3300.0, ef_ttw_per_tonne_fuel={'CO2':3206.0}, gwp100={'CH4':29.8,'N2O':273.0}); `
print('S1_cost=', round(report['S1']['cost'],2)); "
"""
