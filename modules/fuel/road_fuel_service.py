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

we return a small, typed `RoadFuelProfile` with:
  - representative km/L for the *loaded* leg,
  - an average diesel price (R$/L),
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
    - otherwise we try to delegate to an optional helper
      `modules.costs.road_diesel_prices.get_average_price(origin, destiny)`;
    - if that import is missing, we fall back to a conservative constant
      and log a WARNING so you remember to plug the real model.

Public API
----------
- RoadFuelProfile (dataclass)
- get_road_fuel_profile(...)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

from modules.infra.logging import get_logger
from modules.fuel.truck_specs import (
      get_truck_spec
    , guess_axles_from_payload
    , baseline_km_per_l_from_axles
)

_log = get_logger(__name__)

# Optional diesel price helper
try:  # pragma: no cover - defensive import
    from modules.costs.road_diesel_prices import (  # type: ignore
          get_average_price as _get_average_price_helper
    )
    _HAS_PRICE_HELPER = True
except Exception:  # pragma: no cover - defensive import
    _get_average_price_helper = None  # type: ignore
    _HAS_PRICE_HELPER = False

#: Fallback value used only when no explicit price is provided and no helper exists.
#: Adjust once you plug real ANP / model-based values.
_FALLBACK_DIESEL_PRICE_R_PER_L: float = 6.00


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
        Examples: "explicit_override", "road_diesel_prices_helper",
        "fallback_constant".
    extra : Dict[str, Any]
        Free-form metadata bag. Use this if you want to keep, for example,
        the raw helper payload, ANP region code, etc.
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


def _resolve_diesel_price(
      origin: str
    , destiny: str
    , diesel_price_override_r_per_l: Optional[float] = None
) -> tuple[float, str, Dict[str, Any]]:
    """
    Decide which diesel price to use for this O→D pair.

    Resolution order
    ----------------
    1) If caller passed an explicit override → use it and mark source as
       "explicit_override".
    2) Else, if `modules.costs.road_diesel_prices.get_average_price` is
       available → delegate to it and mark source as
       "road_diesel_prices_helper".
    3) Else → return the fallback constant and log a warning once, with
       source "fallback_constant".

    Returns
    -------
    (diesel_price_r_per_liter, price_source, extra)
      - `extra` is a free-form dict; we pass through any metadata we get
        from the helper (if it returns a dict) or leave it empty.
    """
    origin = (origin or "").strip()
    destiny = (destiny or "").strip()

    # 1) Explicit override wins.
    if diesel_price_override_r_per_l is not None:
        value = float(diesel_price_override_r_per_l)
        _log.debug(
            "Using explicit diesel price override: origin=%r destiny=%r price=%.4f R$/L",
            origin,
            destiny,
            value,
        )
        return value, "explicit_override", {}

    # 2) Delegate to helper if available.
    if _HAS_PRICE_HELPER and callable(_get_average_price_helper):  # type: ignore
        try:
            result = _get_average_price_helper(origin=origin, destiny=destiny)  # type: ignore[arg-type]
        except Exception as exc:  # pragma: no cover - defensive
            _log.warning(
                "road_diesel_prices helper raised; falling back to constant. "
                "origin=%r destiny=%r err=%s",
                origin,
                destiny,
                exc,
            )
        else:
            # Helper can return either a bare float or a dict payload with `price_r_per_l`.
            if isinstance(result, dict):
                price = float(result.get("price_r_per_l", _FALLBACK_DIESEL_PRICE_R_PER_L))
                meta: Dict[str, Any] = {k: v for k, v in result.items() if k != "price_r_per_l"}
            else:
                price = float(result)
                meta = {}

            _log.debug(
                "Using diesel price from road_diesel_prices helper: origin=%r destiny=%r price=%.4f R$/L",
                origin,
                destiny,
                price,
            )
            return price, "road_diesel_prices_helper", meta

    # 3) Fallback constant.
    _log.warning(
        "No diesel price helper found; using fallback constant. "
        "origin=%r destiny=%r fallback_price=%.4f R$/L",
        origin,
        destiny,
        _FALLBACK_DIESEL_PRICE_R_PER_L,
    )
    return _FALLBACK_DIESEL_PRICE_R_PER_L, "fallback_constant", {}


def get_road_fuel_profile(
      cargo_t: float
    , origin: str
    , destiny: str
    , *
    , truck_key: str = "auto_by_weight"
    , diesel_price_override_r_per_l: Optional[float] = None
) -> RoadFuelProfile:
    """
    High-level entry point: weight + O/D → km/L + diesel price.

    Parameters
    ----------
    cargo_t : float
        Cargo mass to move (t).
    origin : str
        Origin label (free-form; city, state, or any tag your routing layer uses).
    destiny : str
        Destiny label (free-form, same conventions as `origin`).
    truck_key : str, optional
        Truck preset key into TRUCK_SPECS. Default "auto_by_weight" tries to
        pick a reasonable configuration based on `cargo_t`.
    diesel_price_override_r_per_l : float, optional
        If provided, forces this price (R$/L) and bypasses any helper lookup.

    Returns
    -------
    RoadFuelProfile
        Small, serializable summary that the multimodal builder can consume.
    """
    kmL, resolved_truck_key, axles = _resolve_truck_and_kmL(
          cargo_t
        , truck_key=truck_key
    )

    price, price_source, extra = _resolve_diesel_price(
          origin
        , destiny
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
        "Road fuel profile: origin=%r destiny=%r cargo_t=%.3f "
        "truck_key=%s axles=%s kmL=%.4f diesel_price=%.4f R$/L source=%s",
        profile.origin,
        profile.destiny,
        profile.cargo_t,
        profile.truck_key,
        profile.axles,
        profile.km_per_liter,
        profile.diesel_price_r_per_liter,
        profile.price_source,
    )

    return profile

def main(argv: List[str] | None = None) -> int:
    """
    Small CLI / smoke test for the road fuel service.

    Examples
    --------
    python -m modules.fuel.road_fuel_service
    python -m modules.fuel.road_fuel_service --cargo-t 25 --origin "São Paulo, SP" --destiny "Rio de Janeiro, RJ"
    python -m modules.fuel.road_fuel_service --cargo-t 35 --truck-key bitrain_7ax_36t
    """
    import argparse
    import json
    from dataclasses import asdict

    from modules.infra.logging import init_logging
    from modules.fuel.truck_specs import list_truck_keys

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
          "--truck-key"
        , type=str
        , default="auto_by_weight"
        , choices=["auto_by_weight"] + list_truck_keys()
        , help="Truck preset key or 'auto_by_weight' to infer by payload."
    )
    parser.add_argument(
          "--diesel-price-override"
        , type=float
        , default=None
        , help="Optional explicit diesel price override (R$/L)."
    )

    args = parser.parse_args(argv)

    # Init logging and run
    init_logging()
    _log.info(
        "CLI road fuel service call: cargo_t=%.3f origin=%r destiny=%r truck_key=%s "
        "diesel_price_override=%r",
        args.cargo_t,
        args.origin,
        args.destiny,
        args.truck_key,
        args.diesel_price_override,
    )

    profile = get_road_fuel_profile(
          cargo_t=args.cargo_t
        , origin=args.origin
        , destiny=args.destiny
        , truck_key=args.truck_key
        , diesel_price_override_r_per_l=args.diesel_price_override
    )

    # Pretty-print as JSON so you can pipe it / inspect it
    print(json.dumps(asdict(profile), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
