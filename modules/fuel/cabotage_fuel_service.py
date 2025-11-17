# modules/fuel/cabotage_fuel_service.py
# -*- coding: utf-8 -*-
"""
Cabotage fuel service (sea leg + optional ops/hotel)
====================================================

Purpose
-------
Provide a thin, high-level API to estimate cabotage fuel usage for a
single sea leg between two Brazilian ports, given:

  - origin port name
  - destiny port name
  - cargo mass (t)
  - fuel type (VLSFO default, MFO alternative)

The service returns a small `CabotageFuelProfile` with:
  - sea distance (km) from SeaMatrix,
  - K [kg fuel / (t·km)] used,
  - fuel for sea leg (kg),
  - optional port operations + hotel fuel (kg),
  - total fuel used (kg),
  - some metadata.

Notes
-----
• This module does **not** compute costs or emissions; those are handled
  elsewhere from the fuel_kg outputs.
• Fuel types currently supported:
    - "vlsfo"  (Very Low Sulfur Fuel Oil) — default
    - "mfo"    (residual / marine fuel oil) — alternative
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional, List

import json
import os
import re

from modules.infra.logging import get_logger, init_logging
from modules.cabotage.sea_matrix import SeaMatrix
from modules.ports.ports_index import load_ports

_log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Defaults / fuel types (kept in sync with original evaluator constants)
# ---------------------------------------------------------------------------

#: Sea leg intensity in kg fuel / (tonne·km)
DEFAULT_SEA_K_KG_PER_TKM: float = 0.0027

#: Port ops intensity in kg fuel / tonne handled
DEFAULT_K_PORT_KG_PER_T: float = 0.48

FUEL_TYPE_VLSFO = "vlsfo"
FUEL_TYPE_MFO   = "mfo"

# Same K for both fuel types by default;
# if you calibrate different Ks later, adjust this mapping or pass an override.
_DEFAULT_K_BY_FUEL: Dict[str, float] = {
      FUEL_TYPE_VLSFO: DEFAULT_SEA_K_KG_PER_TKM
    , FUEL_TYPE_MFO  : DEFAULT_SEA_K_KG_PER_TKM
}

DEFAULT_PORTS_JSON       = Path("data/processed/cabotage_data/ports_br.json")
DEFAULT_SEA_MATRIX_JSON  = Path("data/processed/cabotage_data/sea_matrix.json")
DEFAULT_HOTEL_JSON       = Path("data/processed/cabotage_data/hotel.json")


@dataclass(frozen=True)
class CabotageFuelProfile:
    """
    Summary of cabotage fuel usage for a single port→port leg.

    Attributes
    ----------
    origin_port_name : str
        Label used for origin port lookup.
    destiny_port_name : str
        Label used for destiny port lookup.
    cargo_t : float
        Cargo mass in tonnes.
    fuel_type : str
        "vlsfo" (default) or "mfo".
    sea_km : float
        Sea distance between ports (km) from SeaMatrix.
    K_kg_per_tkm : float
        Intensity factor [kg fuel / (t·km)] used for sea leg.
    fuel_sea_kg : float
        Fuel used in the sea leg (kg).
    fuel_ops_hotel_kg : float
        Fuel used in port operations + hotel (kg). Zero if disabled.
    fuel_total_kg : float
        Total fuel used (sea + ops/hotel).
    meta : Dict[str, Any]
        Free-form metadata: port cities, hotel factors, paths, etc.
    """

    origin_port_name: str
    destiny_port_name: str
    cargo_t: float
    fuel_type: str
    sea_km: float
    K_kg_per_tkm: float
    fuel_sea_kg: float
    fuel_ops_hotel_kg: float
    fuel_total_kg: float
    meta: Dict[str, Any]


# ---------------------------------------------------------------------------
# Local helpers (previously from modules.cabotage.accounting)
# ---------------------------------------------------------------------------

def _norm_city(s: str) -> str:
    """Very light normalization for city labels (spaces collapse; accents kept)."""
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def load_hotel_entries(
    *,
    path: str = os.path.join("data", "cabotage_data", "hotel.json"),
) -> dict:
    """
    Load hotel.json payload.

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


def build_hotel_factor_index(
    *,
    hotel_data: dict,
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


def port_fuel_from_handled_mass(
      handled_mass_t: float
    , K_port_kg_per_t: float = DEFAULT_K_PORT_KG_PER_T
) -> float:
    """
    Fuel used in a port call for loading/unloading.

    Parameters
    ----------
    handled_mass_t : float
        Total tonnes actually moved ship↔yard in this call.
    K_port_kg_per_t : float, default DEFAULT_K_PORT_KG_PER_T
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
    _log.debug(
        "port_fuel_from_handled_mass: handled=%.3f t, K=%.3f → fuel=%.3f kg.",
        handled_mass_t,
        K_port_kg_per_t,
        fuel,
    )
    return fuel


# ---------------------------------------------------------------------------
# Internal helpers (matching ports, computing distances)
# ---------------------------------------------------------------------------

def _norm_text(s: str) -> str:
    """Lowercase, strip accents and collapse whitespace for robust matching."""
    import unicodedata

    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _find_port_by_label(
      ports: List[Dict[str, Any]]
    , label: str
) -> Dict[str, Any]:
    """
    Best-effort port lookup by name/city.

    Matching strategy
    -----------------
    1) Exact normalized match against:
         - port["name"]
         - port["city"]
         - f"{city} ({state})"
    2) Fallback: label substring in port["name"] (normalized).

    Raises
    ------
    KeyError if no port is found.
    """
    norm_target = _norm_text(label)
    if not norm_target:
        raise KeyError("Empty origin/destiny port label.")

    # Pass 1: strict normalized equality
    for p in ports:
        city = p.get("city") or ""
        state = p.get("state") or ""
        candidates = [
              p.get("name") or ""
            , city
            , f"{city} ({state})" if city and state else ""
        ]
        for cand in candidates:
            if cand and _norm_text(cand) == norm_target:
                return p

    # Pass 2: substring in "name"
    for p in ports:
        if norm_target in _norm_text(p.get("name") or ""):
            return p

    raise KeyError(f"Port not found for label={label!r}")


def _normalize_fuel_type(fuel_type: str) -> str:
    """
    Map user-provided string to our canonical fuel types.

    Currently accepted:
      - "vlsfo"
      - "mfo"  (you can treat HFO/IFO as "mfo" manually)
    """
    ft = (fuel_type or "").strip().lower()
    if ft == FUEL_TYPE_VLSFO:
        return FUEL_TYPE_VLSFO
    if ft == FUEL_TYPE_MFO:
        return FUEL_TYPE_MFO
    raise ValueError(f"Unsupported fuel_type={fuel_type!r} (use 'vlsfo' or 'mfo').")


def _load_ports_and_matrix(
      ports_json: Path
    , sea_matrix_json: Path
) -> tuple[List[Dict[str, Any]], SeaMatrix]:
    ports = load_ports(path=str(ports_json))
    sea_mx = SeaMatrix.from_json_path(sea_matrix_json)
    _log.info(
        "Loaded ports=%d from %s; SeaMatrix from %s.",
        len(ports),
        ports_json,
        sea_matrix_json,
    )
    return ports, sea_mx


def _sea_distance_km(
      sea_mx: SeaMatrix
    , p_o: Dict[str, Any]
    , p_d: Dict[str, Any]
) -> float:
    """
    Compute coastline-adjusted sea distance using SeaMatrix.

    Expects ports with fields "name", "lat", "lon".
    """
    sea_km = float(
        sea_mx.km(
              {"name": p_o["name"], "lat": float(p_o["lat"]), "lon": float(p_o["lon"])}
            , {"name": p_d["name"], "lat": float(p_d["lat"]), "lon": float(p_d["lon"])}
        )
    )
    _log.info(
        "Sea distance: %s → %s = %.3f km",
        p_o.get("name"),
        p_d.get("name"),
        sea_km,
    )
    return sea_km


def _sea_fuel_kg(
      *
    , sea_km: float
    , cargo_t: float
    , K_kg_per_tkm: float
) -> float:
    """Simple K × tonne-km = fuel_kg."""
    fuel = float(K_kg_per_tkm) * float(cargo_t) * float(sea_km)
    _log.debug(
        "Sea fuel: K=%.6f kg/(t·km) cargo=%.3f t dist=%.3f km → fuel=%.3f kg",
        K_kg_per_tkm,
        cargo_t,
        sea_km,
        fuel,
    )
    return fuel


def _ops_and_hotel_fuel_kg(
      *
    , p_o: Dict[str, Any]
    , p_d: Dict[str, Any]
    , cargo_t: float
    , hotel_json: Path
    , K_port_kg_per_t: float
    , default_hotel_kg_per_t: float
) -> tuple[float, Dict[str, Any]]:
    """
    Approximate port operations + hotel-at-berth fuel.

    Logic:
      - port handling fuel:
            2 × cargo_t × K_port_kg_per_t
        (load at origin + discharge at destiny)
      - hotel fuel:
            cargo_t × (k_o + k_d)
        where k_o / k_d come from hotel.json per-city factors, or fallback
        to `default_hotel_kg_per_t` when missing.
    """
    # Port handling (2 calls: load + discharge)
    fuel_port_origin_kg = port_fuel_from_handled_mass(
          handled_mass_t=cargo_t
        , K_port_kg_per_t=K_port_kg_per_t
    )
    fuel_port_destiny_kg = port_fuel_from_handled_mass(
          handled_mass_t=cargo_t
        , K_port_kg_per_t=K_port_kg_per_t
    )
    fuel_port_total_kg = fuel_port_origin_kg + fuel_port_destiny_kg

    # Hotel factors
    hotel_data = load_hotel_entries(path=str(hotel_json))
    idx = build_hotel_factor_index(hotel_data=hotel_data)

    city_o = _norm_city(p_o.get("city") or "")
    city_d = _norm_city(p_d.get("city") or "")
    k_o = float(idx.get(city_o, default_hotel_kg_per_t))
    k_d = float(idx.get(city_d, default_hotel_kg_per_t))

    fuel_hotel_origin_kg = float(cargo_t) * k_o
    fuel_hotel_destiny_kg = float(cargo_t) * k_d
    fuel_hotel_total_kg   = fuel_hotel_origin_kg + fuel_hotel_destiny_kg

    fuel_total_kg = fuel_port_total_kg + fuel_hotel_total_kg

    _log.info(
        "Ops+hotel fuel: cargo=%.3f t | port=%.3f kg (K_port=%.3f) | "
        "hotel_o=%.3f kg (k_o=%.3f) | hotel_d=%.3f kg (k_d=%.3f) → total=%.3f kg",
        cargo_t,
        fuel_port_total_kg,
        K_port_kg_per_t,
        fuel_hotel_origin_kg,
        k_o,
        fuel_hotel_destiny_kg,
        k_d,
        fuel_total_kg,
    )

    meta = {
          "origin_city": city_o
        , "destiny_city": city_d
        , "port_fuel_total_kg": fuel_port_total_kg
        , "hotel_fuel_total_kg": fuel_hotel_total_kg
        , "hotel_factor_origin_kg_per_t": k_o
        , "hotel_factor_destiny_kg_per_t": k_d
    }
    return fuel_total_kg, meta


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_cabotage_fuel_profile(
      *
    , origin_port_name: str
    , destiny_port_name: str
    , cargo_t: float
    , fuel_type: str = FUEL_TYPE_VLSFO
    , K_kg_per_tkm_override: Optional[float] = None
    , include_ops_and_hotel: bool = True
    , ports_json: Path | str = DEFAULT_PORTS_JSON
    , sea_matrix_json: Path | str = DEFAULT_SEA_MATRIX_JSON
    , hotel_json: Path | str = DEFAULT_HOTEL_JSON
    , K_port_kg_per_t: float = DEFAULT_K_PORT_KG_PER_T
    , default_hotel_kg_per_t: float = 0.0
) -> CabotageFuelProfile:
    """
    High-level entry point: port-port + weight → fuel used (kg).

    Parameters
    ----------
    origin_port_name : str
        Label for origin port (should match ports_br.json names/cities).
    destiny_port_name : str
        Label for destiny port.
    cargo_t : float
        Cargo mass in tonnes.
    fuel_type : str, default "vlsfo"
        Fuel type for the sea leg; currently:
          - "vlsfo"
          - "mfo"
        For now both share the same K default; override via K_kg_per_tkm_override
        if you have better route-/fuel-specific values.
    K_kg_per_tkm_override : float, optional
        If provided, forces K [kg fuel / (t·km)] instead of using defaults.
    include_ops_and_hotel : bool, default True
        If False, only sea fuel is computed (ops/hotel = 0).
    ports_json, sea_matrix_json, hotel_json : Path or str
        Data files used for ports list, SeaMatrix, and hotel factors.
    K_port_kg_per_t : float
        Port ops K [kg fuel / t handled]. Applied to load + discharge.
    default_hotel_kg_per_t : float
        Fallback hotel factor when a city is missing from hotel.json.

    Returns
    -------
    CabotageFuelProfile
        Small, serializable summary that other layers (e.g., multimodal
        builder) can consume.
    """
    cargo_t = float(cargo_t)
    fuel_type_norm = _normalize_fuel_type(fuel_type)

    # Resolve K
    if K_kg_per_tkm_override is not None:
        K_kg_per_tkm = float(K_kg_per_tkm_override)
        K_source = "override"
    else:
        try:
            K_kg_per_tkm = float(_DEFAULT_K_BY_FUEL[fuel_type_norm])
            K_source = "default_by_fuel"
        except KeyError:
            raise ValueError(f"No default K configured for fuel_type={fuel_type_norm!r}")

    ports_json_path = Path(ports_json)
    sea_matrix_path = Path(sea_matrix_json)
    hotel_json_path = Path(hotel_json)

    ports, sea_mx = _load_ports_and_matrix(
          ports_json=ports_json_path
        , sea_matrix_json=sea_matrix_path
    )

    p_o = _find_port_by_label(ports, origin_port_name)
    p_d = _find_port_by_label(ports, destiny_port_name)

    sea_km = _sea_distance_km(sea_mx=sea_mx, p_o=p_o, p_d=p_d)
    fuel_sea_kg = _sea_fuel_kg(
          sea_km=sea_km
        , cargo_t=cargo_t
        , K_kg_per_tkm=K_kg_per_tkm
    )

    if include_ops_and_hotel:
        fuel_ops_hotel_kg, ops_meta = _ops_and_hotel_fuel_kg(
              p_o=p_o
            , p_d=p_d
            , cargo_t=cargo_t
            , hotel_json=hotel_json_path
            , K_port_kg_per_t=K_port_kg_per_t
            , default_hotel_kg_per_t=default_hotel_kg_per_t
        )
    else:
        fuel_ops_hotel_kg = 0.0
        ops_meta = {}

    fuel_total_kg = fuel_sea_kg + fuel_ops_hotel_kg

    meta: Dict[str, Any] = {
          "K_source": K_source
        , "ports_json": str(ports_json_path)
        , "sea_matrix_json": str(sea_matrix_path)
        , "hotel_json": str(hotel_json_path)
        , "K_port_kg_per_t": float(K_port_kg_per_t)
        , "default_hotel_kg_per_t": float(default_hotel_kg_per_t)
        , "origin_port": {
              "name": p_o.get("name")
            , "city": p_o.get("city")
            , "state": p_o.get("state")
            , "lat": float(p_o["lat"])
            , "lon": float(p_o["lon"])
        }
        , "destiny_port": {
              "name": p_d.get("name")
            , "city": p_d.get("city")
            , "state": p_d.get("state")
            , "lat": float(p_d["lat"])
            , "lon": float(p_d["lon"])
        }
    }
    meta.update(ops_meta)

    profile = CabotageFuelProfile(
          origin_port_name=str(origin_port_name)
        , destiny_port_name=str(destiny_port_name)
        , cargo_t=cargo_t
        , fuel_type=fuel_type_norm
        , sea_km=sea_km
        , K_kg_per_tkm=K_kg_per_tkm
        , fuel_sea_kg=fuel_sea_kg
        , fuel_ops_hotel_kg=fuel_ops_hotel_kg
        , fuel_total_kg=fuel_total_kg
        , meta=meta
    )

    _log.info(
        "Cabotage fuel profile: origin=%r destiny=%r cargo_t=%.3f fuel_type=%s "
        "sea_km=%.3f fuel_sea=%.3f kg ops_hotel=%.3f kg total=%.3f kg (K=%.6f, source=%s)",
        profile.origin_port_name,
        profile.destiny_port_name,
        profile.cargo_t,
        profile.fuel_type,
        profile.sea_km,
        profile.fuel_sea_kg,
        profile.fuel_ops_hotel_kg,
        profile.fuel_total_kg,
        profile.K_kg_per_tkm,
        profile.meta.get("K_source"),
    )

    return profile


# ---------------------------------------------------------------------------
# CLI / smoke test
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    """
    Small CLI / smoke test for the cabotage fuel service.

    Examples
    --------
    python -m modules.fuel.cabotage_fuel_service `
        --origin-port "Santos (SP)" `
        --destiny-port "Suape (PE)" `
        --cargo-t 30

    python -m modules.fuel.cabotage_fuel_service `
        --origin-port "Santos (SP)" `
        --destiny-port "Suape (PE)" `
        --cargo-t 30 `
        --fuel-type mfo `
        --K-kg-per-tkm-override 0.0029 `
        --no-ops-hotel
    """
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Cabotage fuel profile (sea leg + optional ops/hotel). "
            "Thin wrapper around get_cabotage_fuel_profile()."
        )
    )
    parser.add_argument(
          "--origin-port"
        , required=True
        , help="Origin port label (as in ports_br.json 'name' or 'city')."
    )
    parser.add_argument(
          "--destiny-port"
        , required=True
        , help="Destiny port label (as in ports_br.json 'name' or 'city')."
    )
    parser.add_argument(
          "--cargo-t"
        , type=float
        , required=True
        , help="Cargo mass to move (tonnes)."
    )
    parser.add_argument(
          "--fuel-type"
        , type=str
        , default=FUEL_TYPE_VLSFO
        , choices=[FUEL_TYPE_VLSFO, FUEL_TYPE_MFO]
        , help="Fuel type for sea leg: 'vlsfo' (default) or 'mfo'."
    )
    parser.add_argument(
          "--K-kg-per-tkm-override"
        , type=float
        , default=None
        , help="Override K [kg fuel / (t·km)] instead of using defaults."
    )

    parser.add_argument(
          "--no-ops-hotel"
        , dest="include_ops_and_hotel"
        , action="store_false"
        , help="Disable port ops + hotel fuel (sea-only)."
    )
    parser.set_defaults(include_ops_and_hotel=True)

    parser.add_argument(
          "--ports-json"
        , type=Path
        , default=DEFAULT_PORTS_JSON
        , help=f"Ports JSON path (default: {DEFAULT_PORTS_JSON})."
    )
    parser.add_argument(
          "--sea-matrix-json"
        , type=Path
        , default=DEFAULT_SEA_MATRIX_JSON
        , help=f"Sea matrix JSON path (default: {DEFAULT_SEA_MATRIX_JSON})."
    )
    parser.add_argument(
          "--hotel-json"
        , type=Path
        , default=DEFAULT_HOTEL_JSON
        , help=f"Hotel factors JSON path (default: {DEFAULT_HOTEL_JSON})."
    )

    parser.add_argument(
          "--K-port-kg-per-t"
        , type=float
        , default=DEFAULT_K_PORT_KG_PER_T
        , help=f"Port ops K [kg fuel / t handled] (default: {DEFAULT_K_PORT_KG_PER_T})."
    )
    parser.add_argument(
          "--default-hotel-kg-per-t"
        , type=float
        , default=0.0
        , help="Fallback hotel factor [kg fuel / t] when city is missing."
    )

    parser.add_argument(
          "--log-level"
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
        , help="Root log level."
    )
    parser.add_argument(
          "--pretty"
        , action="store_true"
        , help="Pretty-print JSON output."
    )

    args = parser.parse_args(argv)

    # Configure logging
    init_logging(
          level=args.log_level
        , force=True
        , write_output=False
    )

    _log.info(
        "CLI cabotage fuel call: origin_port=%r destiny_port=%r cargo_t=%.3f "
        "fuel_type=%s include_ops_and_hotel=%s",
        args.origin_port,
        args.destiny_port,
        args.cargo_t,
        args.fuel_type,
        args.include_ops_and_hotel,
    )

    profile = get_cabotage_fuel_profile(
          origin_port_name=args.origin_port
        , destiny_port_name=args.destiny_port
        , cargo_t=args.cargo_t
        , fuel_type=args.fuel_type
        , K_kg_per_tkm_override=args.K_kg_per_tkm_override
        , include_ops_and_hotel=args.include_ops_and_hotel
        , ports_json=args.ports_json
        , sea_matrix_json=args.sea_matrix_json
        , hotel_json=args.hotel_json
        , K_port_kg_per_t=args.K_port_kg_per_t
        , default_hotel_kg_per_t=args.default_hotel_kg_per_t
    )

    payload = asdict(profile)
    if args.pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
