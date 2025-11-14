# modules/road/fuel_model.py
# -*- coding: utf-8 -*-
"""
Road fuel model (axle-based, ANTT-informed)
===========================================

Purpose
-------
Deterministically estimate **diesel consumption (L)** for a road leg, given:
- distance (km),
- cargo mass to move (t),
- a truck spec (payload_t, axles, ref_weight_t, empty_efficiency_gain),
- an optional **empty backhaul share** (0–1),
- and an optional weight elasticity around a reference weight.

Design notes
------------
- **Baselines (km/L) by axle** come from `modules.road.truck_specs.ANTT_KM_PER_L_BY_AXLES`
  (and, if present, `baseline_km_per_l_from_axles`). A conservative floor is applied for ≥9 axles.
- **Weight effect**: simple linear sensitivity around `ref_weight_t` (km/L ↓ when heavier).
- **Empty backhaul**: improves efficiency by `empty_efficiency_gain` on the empty fraction.
- **Trips**: `ceil(cargo_t / payload_t)` assuming fully loaded trips.

Public API (kept stable)
------------------------
- get_km_l_baseline(axles: int) -> float
- infer_axles_for_payload(payload_t: float) -> int
- adjust_km_per_liter(km_l_baseline: float, cargo_weight_t: float, ref_weight_t: float, *, elasticity: float = 1.0) -> float
- estimate_leg_liters(*, distance_km: float, cargo_t: float, spec: Dict[str, Any], empty_backhaul_share: float = 0.0, elasticity: float = 1.0)
    -> Tuple[float, float, float, int, float, float]
    returns: (liters_total, liters_loaded, liters_empty, trips, kmL_loaded, kmL_empty)

Logging
-------
Uses the project-standard logger. Each function logs inputs/outputs at DEBUG and noteworthy decisions at INFO.
"""

from __future__ import annotations

import math
from typing import Dict, Any, Tuple

from modules.functions._logging import get_logger

# Prefer the centralized truth from truck_specs; fall back gracefully if helpers are missing.
try:
    from modules.fuel.truck_specs import (  # type: ignore
        ANTT_KM_PER_L_BY_AXLES as _ANTT_KM_PER_L_BY_AXLES,
        guess_axles_from_payload as _guess_axles_from_payload,
        baseline_km_per_l_from_axles as _baseline_km_per_l_from_axles,
    )
    _HAS_BASELINE_FN = True
except Exception:  # pragma: no cover - defensive import
    from modules.fuel.truck_specs import (  # type: ignore
        ANTT_KM_PER_L_BY_AXLES as _ANTT_KM_PER_L_BY_AXLES,
        guess_axles_from_payload as _guess_axles_from_payload,
    )
    _baseline_km_per_l_from_axles = None  # type: ignore
    _HAS_BASELINE_FN = False

_log = get_logger(__name__)

# Conservative floor for very heavy CVCs (e.g., ≥9 axles rodotrens)
_CONSERVATIVE_FLOOR_KM_PER_L_FOR_9_PLUS = 1.7

# Clamp bounds for km/L after weight adjustments (sanity guardrails)
_MIN_KM_PER_L_AFTER_ADJUST = 0.6
_MAX_KM_PER_L_AFTER_ADJUST = 8.0


# ────────────────────────────────────────────────────────────────────────────────
# Baseline mapping (axles → km/L)
# ────────────────────────────────────────────────────────────────────────────────
def get_km_l_baseline(axles: int) -> float:
    """
    Return the ANTT-informed baseline km/L for a given axle count.

    Resolution order:
      1) Use truck_specs.baseline_km_per_l_from_axles if available (single source of truth).
      2) Else, use truck_specs.ANTT_KM_PER_L_BY_AXLES with a conservative floor for ≥9 axles.

    Parameters
    ----------
    axles : int
        Number of axles in the road combination.

    Returns
    -------
    float
        Baseline km/L for planning.

    Raises
    ------
    KeyError
        If no baseline is configured for the given axle count (<9 and missing in the table).
    """
    axles = int(axles)
    if _HAS_BASELINE_FN and callable(_baseline_km_per_l_from_axles):  # type: ignore
        km_l = float(_baseline_km_per_l_from_axles(axles))  # delegate to truck_specs
        _log.debug(f"get_km_l_baseline(axles={axles}) → {km_l:.4f} (via truck_specs helper)")
        return km_l

    if axles >= 9:
        _log.debug(
            "get_km_l_baseline: axles ≥ 9 → using conservative floor "
            f"{_CONSERVATIVE_FLOOR_KM_PER_L_FOR_9_PLUS:.2f} km/L."
        )
        return _CONSERVATIVE_FLOOR_KM_PER_L_FOR_9_PLUS

    try:
        km_l = float(_ANTT_KM_PER_L_BY_AXLES[axles])
        _log.debug(f"get_km_l_baseline(axles={axles}) → {km_l:.4f} (table fallback)")
        return km_l
    except KeyError as e:
        _log.error(f"No baseline km/L configured for {axles} axles.")
        raise e


# ────────────────────────────────────────────────────────────────────────────────
# Axle inference
# ────────────────────────────────────────────────────────────────────────────────
def infer_axles_for_payload(payload_t: float) -> int:
    """
    Proxy retained for backward-compatibility. Delegates to `truck_specs.guess_axles_from_payload`.

    Parameters
    ----------
    payload_t : float
        Intended payload (tonnes) per loaded trip.

    Returns
    -------
    int
        Recommended axle count.

    Notes
    -----
    This is a *payload-based* heuristic (carga útil), not PBTC. It’s intentionally conservative.
    """
    ax = int(_guess_axles_from_payload(float(payload_t)))
    _log.debug(f"infer_axles_for_payload(payload_t={payload_t}) → {ax} axles")
    return ax


# ────────────────────────────────────────────────────────────────────────────────
# Weight sensitivity
# ────────────────────────────────────────────────────────────────────────────────
def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def adjust_km_per_liter(
    km_l_baseline: float,
    cargo_weight_t: float,
    ref_weight_t: float,
    *,
    elasticity: float = 1.0,
) -> float:
    """
    Linearly adjust km/L around a reference loaded weight.

    Heavier-than-reference payloads reduce km/L; lighter increase it (bounded by guardrails).

    Parameters
    ----------
    km_l_baseline : float
        Baseline km/L for the given axle configuration (from ANTT table).
    cargo_weight_t : float
        Effective loaded weight used for sensitivity (typically the truck's *payload_t*).
    ref_weight_t : float
        Reference loaded weight that defines the neutral point.
    elasticity : float, default 1.0
        Linear multiplier for sensitivity. Set <1 to soften, >1 to amplify.

    Returns
    -------
    float
        Adjusted km/L for the **loaded** condition, clamped to reasonable bounds.
    """
    try:
        km_l_baseline = float(km_l_baseline)
        cargo_weight_t = float(cargo_weight_t)
        ref_weight_t = float(ref_weight_t)
        elasticity = float(elasticity)
    except Exception as e:  # pragma: no cover - defensive
        _log.warning(f"adjust_km_per_liter: type coercion failed; passthrough baseline. err={e}")
        return km_l_baseline

    if ref_weight_t <= 0:
        _log.debug("adjust_km_per_liter: ref_weight_t ≤ 0 → returning baseline (no adjustment).")
        return km_l_baseline

    delta = (cargo_weight_t - ref_weight_t) / ref_weight_t
    km_l = km_l_baseline * (1.0 - elasticity * delta)

    km_l_clamped = _clamp(km_l, _MIN_KM_PER_L_AFTER_ADJUST, _MAX_KM_PER_L_AFTER_ADJUST)
    _log.debug(
        "adjust_km_per_liter: "
        f"baseline={km_l_baseline:.4f}, cargo={cargo_weight_t:.3f} t, ref={ref_weight_t:.3f} t, "
        f"elasticity={elasticity:.3f} → kmL_loaded={km_l:.4f}, clamped={km_l_clamped:.4f}"
    )
    return km_l_clamped


# ────────────────────────────────────────────────────────────────────────────────
# Main estimator
# ────────────────────────────────────────────────────────────────────────────────
def estimate_leg_liters(
    *,
    distance_km: float,
    cargo_t: float,
    spec: Dict[str, Any],
    empty_backhaul_share: float = 0.0,
    elasticity: float = 1.0,
) -> Tuple[float, float, float, int, float, float]:
    """
    Estimate diesel consumption for a **single road leg** with possible empty return.

    Parameters
    ----------
    distance_km : float
        One-way distance in km for the road leg (origin → destination).
    cargo_t : float
        Total cargo mass to move (tonnes). Trips will be `ceil(cargo_t / payload_t)`.
    spec : Dict[str, Any]
        Truck spec dictionary. Expected keys (with safe defaults if absent):
            - 'payload_t' (float): nominal payload per trip. Default: 27.0
            - 'axles' (int): axle count. If missing, inferred from payload.
            - 'ref_weight_t' (float): reference loaded weight for sensitivity. Default: 20.0
            - 'empty_efficiency_gain' (float): fractional gain for empty trips. Default: 0.18
            - 'label' (str): human-readable name (optional, for logs only).
    empty_backhaul_share : float, default 0.0
        Fraction (0–1) of **trips** that return empty along the same distance.
        Example: 0.5 → half of the loaded trips come back empty.
    elasticity : float, default 1.0
        Linear sensitivity multiplier for weight vs. km/L.

    Returns
    -------
    Tuple[float, float, float, int, float, float]
        (liters_total, liters_loaded, liters_empty, trips, kmL_loaded, kmL_empty)

    Notes
    -----
    - A distance of 0 km or cargo_t ≤ 0 short-circuits to zeros with trips=0.
    - `empty_backhaul_share` is clamped to [0, 1] for safety.
    - This function does *not* price fuel or compute emissions; see `modules.road.emissions`.
    """
    # ── Coerce & validate inputs
    try:
        distance_km = float(distance_km)
        cargo_t = float(cargo_t)
    except Exception as e:  # pragma: no cover - defensive
        _log.error(f"estimate_leg_liters: non-numeric inputs: distance_km={distance_km}, cargo_t={cargo_t}; err={e}")
        raise

    if distance_km <= 0.0 or cargo_t <= 0.0:
        _log.info(
            "estimate_leg_liters: non-positive distance or cargo. "
            f"distance_km={distance_km}, cargo_t={cargo_t} → returning zeros."
        )
        return (0.0, 0.0, 0.0, 0, 0.0, 0.0)

    # ── Extract spec with safe fallbacks
    payload_t = float(spec.get("payload_t", 27.0))
    ref_weight_t = float(spec.get("ref_weight_t", 20.0))
    empty_eff_gain = float(spec.get("empty_efficiency_gain", 0.18))
    label = str(spec.get("label", "")) or f"axles={spec.get('axles', 'auto')}"

    # Resolve axles (given > spec > infer)
    axles = spec.get("axles")
    if axles is None:
        axles = infer_axles_for_payload(payload_t)
        _log.info(
            f"estimate_leg_liters: axles not provided in spec → inferred {axles} axles from payload_t={payload_t} t."
        )
    axles = int(axles)

    # Trips (ceil)
    trips = max(1, math.ceil(cargo_t / max(payload_t, 1e-9)))

    # Clamp backhaul share to [0, 1]
    empty_backhaul_share = _clamp(float(empty_backhaul_share), 0.0, 1.0)

    # ── Baseline and adjustments
    kmL_base = get_km_l_baseline(axles)
    kmL_loaded = adjust_km_per_liter(
        km_l_baseline=kmL_base,
        cargo_weight_t=payload_t,   # sensitivity around declared payload capacity
        ref_weight_t=ref_weight_t,
        elasticity=elasticity,
    )
    kmL_empty = kmL_loaded * (1.0 + empty_eff_gain)

    # ── Liters (loaded + empty backhaul fraction)
    liters_loaded = (distance_km / kmL_loaded) * trips
    liters_empty = (distance_km / kmL_empty) * (trips * empty_backhaul_share)
    liters_total = liters_loaded + liters_empty

    # ── Logs (inputs → outputs)
    _log.debug(
        "estimate_leg_liters.inputs: "
        f"label='{label}', axles={axles}, distance_km={distance_km:.3f}, cargo_t={cargo_t:.3f}, "
        f"payload_t={payload_t:.3f}, ref_weight_t={ref_weight_t:.3f}, empty_eff_gain={empty_eff_gain:.3f}, "
        f"empty_backhaul_share={empty_backhaul_share:.3f}, elasticity={elasticity:.3f}"
    )
    _log.info(
        "estimate_leg_liters.result: "
        f"trips={trips}, kmL_base={kmL_base:.4f}, kmL_loaded={kmL_loaded:.4f}, kmL_empty={kmL_empty:.4f}, "
        f"liters_loaded={liters_loaded:.4f}, liters_empty={liters_empty:.4f}, liters_total={liters_total:.4f}"
    )

    return (
        float(liters_total),
        float(liters_loaded),
        float(liters_empty),
        int(trips),
        float(kmL_loaded),
        float(kmL_empty),
    )


"""
Quick logging smoke test (PowerShell)
python -c `
"from modules.functions.logging import init_logging; `
from modules.road.fuel_model import estimate_leg_liters; `
init_logging(level='INFO', force=True, write_output=False); `
spec = {'label':'carreta 5e','payload_t':27.0,'axles':5,'ref_weight_t':20.0,'empty_efficiency_gain':0.18}; `
out = estimate_leg_liters(distance_km=120.0, cargo_t=40.0, spec=spec, empty_backhaul_share=0.5); `
print('OUT =', out); "
"""
