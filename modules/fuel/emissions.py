# modules/fuel/emissions.py
# -*- coding: utf-8 -*-
"""
Fuel → CO₂e converter
=====================

Purpose
-------
Small, self-contained helper to convert an amount of fuel (by **mass in kg**)
into CO₂e emissions using simple, documented TTW (tank-to-wheel) factors.

This is deliberately generic and independent of routing logic. You can plug in:

- road diesel from ``road_fuel_service`` (``fuel_kg`` or ``fuel_liters``)
- cabotage fuel from ``cabotage_fuel_service`` (``fuel_total_kg``)
- any other pre-computed fuel mass in kg.

Public API
----------
- get_ef_kg_per_kg(fuel_type: str) -> float
- estimate_fuel_emissions(
      fuel_mass_kg: float,
      fuel_type: str,
      ef_kg_per_kg_override: float | None = None,
  ) -> dict

Return shape
------------
estimate_fuel_emissions(...) returns a compact dict:

{
    "fuel_type":          "diesel",          # canonical key
    "input_fuel_type":    "Diesel B7 BR",    # whatever the caller passed
    "fuel_mass_kg":       207.20,
    "ef_kg_per_kg":       3.15,
    "co2e_kg":            653.0,
}

Assumptions
-----------
- Factors are **TTW** and CO₂-dominated (CH₄/N₂O ignored; they are small).
- Values are rounded, planning-level defaults, loosely based on IPCC/IMO /
  national inventory guidelines. They are *not* meant for audited inventories.
- You can always override the factor with `ef_kg_per_kg_override`.

Canonical fuel keys
-------------------
We group fuels in two broad baskets:

- "diesel-like" (distillates):  diesel, mgo, mdo, gasoil
- "residual / fuel oil":        vlsfo, mfo, hfo, ifo

All of these share a single default factor per basket. Aliases (e.g. "VLSFO 0.5")
are normalised internally to a canonical key.

If you need very specific factors, pass `ef_kg_per_kg_override=...` yourself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from modules.infra.logging import get_logger

log = get_logger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# Data structures & defaults
# ────────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FuelFactor:
    """
    Simple container for a fuel's TTW CO₂e factor.

    ef_kg_per_kg : float
        Emission factor in kg CO₂e per kg of fuel burned (TTW).
    group : str
        Coarse group label ("diesel_like", "residual").
    """
    key: str
    ef_kg_per_kg: float
    group: str
    label: str


# Canonical factors (TTW, rounded; CO₂-dominated).
# If you ever want to refine them, edit here and keep the rest of the code intact.
_CANONICAL_FACTORS: Dict[str, FuelFactor] = {
    "diesel": FuelFactor(
          key="diesel"
        , ef_kg_per_kg=3.15      # ≈ 3.15 kg CO₂e / kg diesel-like fuel
        , group="diesel_like"
        , label="Road diesel / MGO"
    )
    , "mgo": FuelFactor(
          key="mgo"
        , ef_kg_per_kg=3.15
        , group="diesel_like"
        , label="Marine gas oil"
    )
    , "mdo": FuelFactor(
          key="mdo"
        , ef_kg_per_kg=3.15
        , group="diesel_like"
        , label="Marine diesel oil"
    )
    , "gasoil": FuelFactor(
          key="gasoil"
        , ef_kg_per_kg=3.15
        , group="diesel_like"
        , label="Gasoil / distillate"
    )
    , "vlsfo": FuelFactor(
          key="vlsfo"
        , ef_kg_per_kg=3.21      # ≈ 3.2 kg CO₂e / kg residual fuel oil
        , group="residual"
        , label="Very low sulphur fuel oil (VLSFO)"
    )
    , "mfo": FuelFactor(
          key="mfo"
        , ef_kg_per_kg=3.21
        , group="residual"
        , label="Marine fuel oil"
    )
    , "hfo": FuelFactor(
          key="hfo"
        , ef_kg_per_kg=3.21
        , group="residual"
        , label="Heavy fuel oil (HFO)"
    )
    , "ifo": FuelFactor(
          key="ifo"
        , ef_kg_per_kg=3.21
        , group="residual"
        , label="Intermediate fuel oil (IFO)"
    )
}


# Aliases → canonical keys (lowercase + underscores).
_FUEL_ALIASES: Dict[str, str] = {
      "diesel_b7": "diesel"
    , "diesel_b7_br": "diesel"
    , "road_diesel": "diesel"
    , "diesel": "diesel"

    , "mgo": "mgo"
    , "marine_gas_oil": "mgo"

    , "mdo": "mdo"
    , "marine_diesel_oil": "mdo"

    , "gasoil": "gasoil"

    , "vlsfo": "vlsfo"
    , "vlsfo_0.5": "vlsfo"
    , "very_low_sulphur_fuel_oil": "vlsfo"

    , "mfo": "mfo"
    , "fuel_oil": "mfo"
    , "residual_fuel_oil": "mfo"

    , "hfo": "hfo"
    , "heavy_fuel_oil": "hfo"

    , "ifo": "ifo"
    , "intermediate_fuel_oil": "ifo"
}


# ────────────────────────────────────────────────────────────────────────────────
# Normalisation helpers
# ────────────────────────────────────────────────────────────────────────────────


def _normalise_fuel_type(fuel_type: str) -> str:
    """
    Normalise a free-text fuel_type into a canonical key.

    Behaviour
    ---------
    - Lowercases and replaces spaces/hyphens with underscores.
    - Looks up in `_FUEL_ALIASES`; if missing, returns the cleaned key itself.
    """
    key = fuel_type.strip().lower()
    key = key.replace(" ", "_").replace("-", "_")
    canonical = _FUEL_ALIASES.get(key, key)

    if canonical not in _CANONICAL_FACTORS:
        # Unknown but we still return `canonical` so the caller can decide what to do.
        log.warning(
              "emissions._normalise_fuel_type: unknown fuel_type %r → canonical key %r not in factor table. "
              "You will need to pass ef_kg_per_kg_override explicitly."
            , fuel_type
            , canonical
        )

    return canonical


# ────────────────────────────────────────────────────────────────────────────────
# Public factor accessor
# ────────────────────────────────────────────────────────────────────────────────


def get_ef_kg_per_kg(fuel_type: str) -> float:
    """
    Return the default TTW emission factor for a given fuel type (kg CO₂e / kg fuel).

    Parameters
    ----------
    fuel_type : str
        Free-text fuel label (e.g. 'diesel', 'VLSFO 0.5', 'MGO').

    Raises
    ------
    KeyError
        If the normalised key is not present in `_CANONICAL_FACTORS`.
    """
    canonical = _normalise_fuel_type(fuel_type)

    if canonical not in _CANONICAL_FACTORS:
        raise KeyError(
            f"Unknown fuel_type={fuel_type!r} (canonical key={canonical!r}); "
            f"known canonical keys: {sorted(_CANONICAL_FACTORS.keys())}"
        )

    return _CANONICAL_FACTORS[canonical].ef_kg_per_kg


# ────────────────────────────────────────────────────────────────────────────────
# Main converter
# ────────────────────────────────────────────────────────────────────────────────


def estimate_fuel_emissions(
      *
    , fuel_mass_kg: float
    , fuel_type: str
    , ef_kg_per_kg_override: Optional[float] = None
) -> Dict[str, Any]:
    """
    Convert fuel mass (kg) and type into CO₂e (kg) using TTW factors.

    Parameters
    ----------
    fuel_mass_kg : float
        Mass of fuel burned [kg]. Must be >= 0.
    fuel_type : str
        Free-text fuel label. Normalised to a canonical key internally.
    ef_kg_per_kg_override : Optional[float]
        If provided, this factor (kg CO₂e / kg fuel) is used instead of the
        built-in defaults, but the canonical key is still resolved and returned
        for metadata.

    Returns
    -------
    Dict[str, Any]
        {
          "fuel_type":       "<canonical key>",
          "input_fuel_type": "<original string>",
          "fuel_mass_kg":    <float>,
          "ef_kg_per_kg":    <float>,
          "co2e_kg":         <float>,
        }
    """
    fuel_mass_kg = float(fuel_mass_kg)
    if fuel_mass_kg < 0:
        raise ValueError("fuel_mass_kg must be >= 0.")

    canonical = _normalise_fuel_type(fuel_type)

    if ef_kg_per_kg_override is not None:
        ef = float(ef_kg_per_kg_override)
    else:
        ef = get_ef_kg_per_kg(canonical)

    co2e_kg = fuel_mass_kg * ef

    log.debug(
          "estimate_fuel_emissions: fuel_mass_kg=%.4f, input_fuel_type=%r, canonical=%r, ef_kg_per_kg=%.4f "
          "→ co2e_kg=%.4f"
        , fuel_mass_kg
        , fuel_type
        , canonical
        , ef
        , co2e_kg
    )

    return {
          "fuel_type": canonical
        , "input_fuel_type": fuel_type
        , "fuel_mass_kg": fuel_mass_kg
        , "ef_kg_per_kg": ef
        , "co2e_kg": co2e_kg
    }


# ────────────────────────────────────────────────────────────────────────────────
# Tiny CLI / smoke test
# ────────────────────────────────────────────────────────────────────────────────


def main(argv: Optional[list[str]] = None) -> int:
    """
    Minimal CLI for quick checks, e.g.:

    python -m modules.fuel.emissions `
        --fuel-kg 207.201 `
        --fuel-type vlsfo `
        --pretty
    """
    import argparse
    import json

    from modules.infra.logging import init_logging

    parser = argparse.ArgumentParser(
        description="Convert a fuel mass [kg] into CO₂e [kg] given a fuel type."
    )
    parser.add_argument(
          "--fuel-kg"
        , type=float
        , required=True
        , help="Fuel mass burned [kg]."
    )
    parser.add_argument(
          "--fuel-type"
        , type=str
        , required=True
        , help="Fuel type label (e.g. 'diesel', 'vlsfo', 'mgo')."
    )
    parser.add_argument(
          "--ef-override"
        , type=float
        , default=None
        , help="Override factor [kg CO₂e / kg fuel]. If set, skips defaults."
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
          "CLI fuel→CO₂e call: fuel_mass_kg=%.4f fuel_type=%r ef_override=%r"
        , args.fuel_kg
        , args.fuel_type
        , args.ef_override
    )

    result = estimate_fuel_emissions(
          fuel_mass_kg=args.fuel_kg
        , fuel_type=args.fuel_type
        , ef_kg_per_kg_override=args.ef_override
    )

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
