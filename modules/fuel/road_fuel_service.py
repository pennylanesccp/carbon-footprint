# modules/fuel/road_fuel_service.py
# -*- coding: utf-8 -*-
"""
High-level road fuel lookup (km/L + diesel price)
=================================================

This module exposes a *very small* API that other layers can call when they
need road fuel assumptions but don't want to know the details about:
  - truck presets / axles (TRUCK_SPECS, ANTT, etc.),
  - how km/L is derived from the payload,
  - where diesel prices come from.

Core idea
---------
Given:
  - cargo mass to move (t),
  - origin label (string),
  - destiny label (string),
  - origin UF code (e.g. 'SP'),
  - destiny UF code (e.g. 'CE'),

we return a small, typed `RoadFuelProfile` with:
  - representative km/L for the *loaded* leg,
  - an average diesel price (R$/L) derived from the UFs,
  - the resolved truck preset and axles used,
  - some metadata that can be logged or written to CSV.

The **multimodal builder** can then combine:
  - this `km_per_liter` with its own distance (km),
  - this `diesel_price_r_per_liter` with the liters estimate,

to obtain fuel consumption and cost.

Design notes
------------
• We intentionally keep this module thin and deterministic.
  It should be very cheap to call many times.

• km/L:
    - we reuse the centralized helpers from `modules.fuel.truck_specs`
      (`guess_axles_from_payload`, `baseline_km_per_l_from_axles`).
    - for now we *do not* model empty backhaul here; that lives in the
      more detailed `road_fuel_model` if you need it.

• Diesel price:
    - caller may optionally *override* the diesel price explicitly;
    - otherwise we delegate to `modules.costs.diesel_prices.get_average_price`
      using **only** the origin/destiny UFs;
    - the helper itself takes care of CSV loading and fallback to a default
      price if needed.

Public API
----------
- RoadFuelProfile (dataclass)
- get_road_fuel_profile(...)
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, List

from modules.infra.logging import get_logger
from modules.fuel.truck_specs import (
      get_truck_spec
    , guess_axles_from_payload
    , baseline_km_per_l_from_axles
    , list_truck_keys
)
from modules.costs.diesel_prices import get_average_price

_log = get_logger(__name__)


# ───────────────────────────── dataclass model ────────────────────────────────


@dataclass(frozen=True)
class RoadFuelProfile:
    """
    Small, serializable summary of road fuel assumptions for a given O→D leg.

    Attributes
    ----------
    cargo_t : float
        Cargo mass (t) that this profile was derived for. This is mainly used
        to guess a representative truck preset / axle count when `truck_key`
        is left as "auto_by_weight".
    origin : str
        Origin label passed by the caller (city, state, or any free-form tag).
    destiny : str
        Destiny label passed by the caller.
    km_per_liter : float
        Representative km/L for the *loaded* leg, already adjusted using
        `baseline_km_per_l_from_axles` for the resolved axle count.
    diesel_price_r_per_liter : float
        Average diesel price used for this O→D leg (R$/L).
    truck_key : str
        Name of the truck preset used (e.g. "rodotrem_9ax_48t" or
        "auto_by_weight").
    axles : int
        Axle count effectively used to derive `km_per_liter`.
    price_source : str
        Short, human-readable description of *where* the price came from.
        Examples: "explicit_override", "default_price_param",
        "latest_diesel_prices_csv".
    extra : Dict[str, Any]
        Free-form metadata bag. Typically contains UF-level info and CSV path.
    """

    cargo_t: float
    origin: str
    destiny: str
    km_per_liter: float
    diesel_price_r_per_liter: float
    truck_key: str
    axles: int
    price_source: str
    extra: Dict[str, Any]


# ───────────────────────── truck / kmL resolution ─────────────────────────────


def _resolve_truck_and_kmL(
      cargo_t: float
    , truck_key: str = "auto_by_weight"
) -> tuple[float, str, int]:
    """
    Internal helper that chooses a truck preset and km/L for the *loaded* leg.

    Parameters
    ----------
    cargo_t : float
        Cargo mass to move (t).
    truck_key : str, optional
        Predefined key into TRUCK_SPECS. If set to "auto_by_weight", we try to
        infer the axle count from `cargo_t` using `guess_axles_from_payload`
        and *do not* enforce any hard payload limit here (that belongs in a
        separate validation layer).

    Returns
    -------
    (km_per_liter, resolved_truck_key, axles)
    """
    cargo_t = float(cargo_t)
    truck_key = truck_key or "auto_by_weight"

    if truck_key == "auto_by_weight":
        # Let the helper guess a realistic axle count and then derive km/L.
        axles = int(guess_axles_from_payload(cargo_t))
        kmL = float(baseline_km_per_l_from_axles(axles))
        _log.debug(
            "Resolved truck via auto_by_weight: cargo_t=%.3f → axles=%s, kmL=%.4f",
            cargo_t,
            axles,
            kmL,
        )
        return kmL, truck_key, axles

    # Otherwise, we trust an explicit preset.
    spec = get_truck_spec(truck_key)
    axles = int(spec["axles"])
    kmL = float(baseline_km_per_l_from_axles(axles))

    _log.debug(
        "Resolved truck via explicit preset: truck_key=%s, cargo_t=%.3f → axles=%s, kmL=%.4f",
        truck_key,
        cargo_t,
        axles,
        kmL,
    )
    return kmL, truck_key, axles


# ───────────────────── diesel price resolution (UF-only) ──────────────────────


def _resolve_diesel_price_by_uf(
      uf_o: str
    , uf_d: str
    , diesel_price_override_r_per_l: Optional[float] = None
) -> tuple[float, str, Dict[str, Any]]:
    """
    Decide which diesel price to use for this O→D pair, using only UFs.

    Resolution order
    ----------------
    1) If caller passed an explicit override → use it and mark source as
       "explicit_override".
    2) Else, delegate to `modules.costs.diesel_prices.get_average_price(uf_o, uf_d)`.
       That helper is responsible for:
         - loading the latest CSV,
         - looking up both UFs,
         - falling back to a default price when needed.

    Returns
    -------
    (diesel_price_r_per_liter, price_source, extra)
      - `extra` is a free-form dict; we pass through any metadata we get
        from the helper, such as uf_origin/uf_destiny, price_origin,
        price_destiny, CSV path, etc.
    """
    uf_o_norm = (uf_o or "").upper().strip()
    uf_d_norm = (uf_d or "").upper().strip()

    # 1) Explicit override wins.
    if diesel_price_override_r_per_l is not None:
        value = float(diesel_price_override_r_per_l)
        _log.debug(
            "Using explicit diesel price override: uf_o=%r uf_d=%r price=%.4f R$/L",
            uf_o_norm,
            uf_d_norm,
            value,
        )
        return value, "explicit_override", {}

    # 2) Delegate to the diesel_prices helper (UF-based logic lives there).
    meta = get_average_price(
          uf_o=uf_o_norm
        , uf_d=uf_d_norm
    )

    # Helper returns a metadata dict with at least 'price_r_per_l' and 'source'.
    price = float(meta.get("price_r_per_l", 0.0))
    price_source = str(meta.get("source", "diesel_prices_helper"))
    extra: Dict[str, Any] = {
          k: v
        for k, v in meta.items()
        if k not in {"price_r_per_l", "source"}
    }

    _log.debug(
        "Using diesel price from diesel_prices helper: uf_o=%r uf_d=%r price=%.4f R$/L source=%s",
        uf_o_norm,
        uf_d_norm,
        price,
        price_source,
    )
    return price, price_source, extra


# ───────────────────────────── public entry point ─────────────────────────────


def get_road_fuel_profile(
      cargo_t: float
    , origin: str
    , destiny: str
    , uf_o: str
    , uf_d: str
    , *
    , truck_key: str = "auto_by_weight"
    , diesel_price_override_r_per_l: Optional[float] = None
) -> RoadFuelProfile:
    """
    High-level entry point: weight + O/D + UFs → km/L + diesel price.

    Parameters
    ----------
    cargo_t : float
        Cargo mass to move (t).
    origin : str
        Origin label (free-form; city, state, or any tag your routing layer uses).
        Used only for logging / JSON output – *not* for price calculation.
    destiny : str
        Destiny label (free-form, same conventions as `origin`).
        Used only for logging / JSON output – *not* for price calculation.
    uf_o : str
        Origin UF code (e.g. 'SP').
    uf_d : str
        Destiny UF code (e.g. 'CE').
    truck_key : str, optional
        Truck preset key into TRUCK_SPECS. Default "auto_by_weight" tries to
        pick a reasonable configuration based on `cargo_t`.
    diesel_price_override_r_per_l : float, optional
        If provided, forces this price (R$/L) and bypasses CSV lookup.

    Returns
    -------
    RoadFuelProfile
        Small, serializable summary that the multimodal builder can consume.
    """
    kmL, resolved_truck_key, axles = _resolve_truck_and_kmL(
          cargo_t
        , truck_key=truck_key
    )

    price, price_source, extra = _resolve_diesel_price_by_uf(
          uf_o=uf_o
        , uf_d=uf_d
        , diesel_price_override_r_per_l=diesel_price_override_r_per_l
    )

    profile = RoadFuelProfile(
          cargo_t=float(cargo_t)
        , origin=str(origin)
        , destiny=str(destiny)
        , km_per_liter=kmL
        , diesel_price_r_per_liter=price
        , truck_key=resolved_truck_key
        , axles=axles
        , price_source=price_source
        , extra=extra
    )

    _log.info(
        "Road fuel profile: origin=%r destiny=%r uf_o=%s uf_d=%s cargo_t=%.3f "
        "truck_key=%s axles=%s kmL=%.4f diesel_price=%.4f R$/L source=%s",
        profile.origin,
        profile.destiny,
        uf_o,
        uf_d,
        profile.cargo_t,
        profile.truck_key,
        profile.axles,
        profile.km_per_liter,
        profile.diesel_price_r_per_liter,
        profile.price_source,
    )

    return profile


# ─────────────────────────────── CLI smoke test ───────────────────────────────


def main(argv: List[str] | None = None) -> int:
    """
    Small CLI / smoke test for the road fuel service.

    Examples
    --------
    python -m modules.fuel.road_fuel_service
    python -m modules.fuel.road_fuel_service --cargo-t 25 --origin "São Paulo, SP" --destiny "Rio de Janeiro, RJ" --uf-origin SP --uf-destiny RJ
    python -m modules.fuel.road_fuel_service --cargo-t 35 --truck-key bitrain_7ax_36t
    """
    import argparse
    import json

    from modules.infra.logging import init_logging

    parser = argparse.ArgumentParser(
        description=(
            "High-level road fuel profile (km/L + diesel price). "
            "This is a thin wrapper around get_road_fuel_profile()."
        )
    )
    parser.add_argument(
          "--cargo-t"
        , type=float
        , default=20.0
        , help="Cargo mass to move (t)."
    )
    parser.add_argument(
          "--origin"
        , type=str
        , default="São Paulo, SP"
        , help="Origin label (city/state or free-form tag)."
    )
    parser.add_argument(
          "--destiny"
        , type=str
        , default="Rio de Janeiro, RJ"
        , help="Destiny label (city/state or free-form tag)."
    )
    parser.add_argument(
          "--uf-origin"
        , dest="uf_origin"
        , type=str
        , default="SP"
        , help="Origin UF code (e.g. 'SP')."
    )
    parser.add_argument(
          "--uf-destiny"
        , dest="uf_destiny"
        , type=str
        , default="RJ"
        , help="Destiny UF code (e.g. 'RJ')."
    )
    parser.add_argument(
          "--truck-key"
        , type=str
        , default="auto_by_weight"
        , choices=["auto_by_weight"] + sorted(list_truck_keys())
        , help="Truck preset key or 'auto_by_weight' to infer by payload."
    )
    parser.add_argument(
          "--diesel-price-override"
        , type=float
        , default=None
        , help="Optional explicit diesel price override (R$/L)."
    )
    parser.add_argument(
          "--log-level"
        , type=str
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
        , help="Logging level for this smoke test."
    )

    args = parser.parse_args(argv)

    init_logging(
          level=args.log_level
        , force=True
        , write_output=False
    )

    _log.info(
        "CLI road fuel service call: cargo_t=%.3f origin=%r destiny=%r uf_o=%s uf_d=%s "
        "truck_key=%s diesel_price_override=%r",
        args.cargo_t,
        args.origin,
        args.destiny,
        args.uf_origin,
        args.uf_destiny,
        args.truck_key,
        args.diesel_price_override,
    )

    profile = get_road_fuel_profile(
          cargo_t=args.cargo_t
        , origin=args.origin
        , destiny=args.destiny
        , uf_o=args.uf_origin
        , uf_d=args.uf_destiny
        , truck_key=args.truck_key
        , diesel_price_override_r_per_l=args.diesel_price_override
    )

    print(json.dumps(asdict(profile), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
