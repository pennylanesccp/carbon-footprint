# -*- coding: utf-8 -*-
"""
Axle-based road fuel model
--------------------------
- Baseline km/L from ANTT-style tables by axle count (containerized cargo).
- Linear weight adjustment around a 'reference loaded weight'.
- Empty backhaul handled via an 'empty_efficiency_gain' factor.
"""

from __future__ import annotations

import math
from typing import Dict, Any, Tuple

# ────────────────────────────────────────────────────────────────────────────────
# Baseline km/L by axle count (containerized cargo) — planning values
# Tune if you calibrate with telemetry later.
# ────────────────────────────────────────────────────────────────────────────────
_ANTT_KM_PER_L_BASELINE: Dict[int, float] = {
      2: 4.0
    , 3: 3.0
    , 4: 2.7
    , 5: 2.3
    , 6: 2.0
    , 7: 2.0
    # 8 not typical for container combos; 9+ grouped below
}

def get_km_l_baseline(axles: int) -> float:
    if axles >= 9:
        return 1.7
    if axles in _ANTT_KM_PER_L_BASELINE:
        return _ANTT_KM_PER_L_BASELINE[axles]
    raise KeyError(f"No baseline km/L configured for {axles} axles.")

def adjust_km_per_liter(
      km_l_baseline: float
    , cargo_weight_t: float
    , ref_weight_t: float
    , *
    , elasticity: float = 1.0
) -> float:
    """
    Linear sensitivity around 'ref_weight_t'. Heavier payload ⇒ lower km/L.
    """
    if ref_weight_t <= 0:
        return km_l_baseline
    delta = (cargo_weight_t - ref_weight_t) / ref_weight_t
    km_l = km_l_baseline * (1.0 - elasticity * delta)
    # keep km/L within reasonable physical bounds
    return max(0.6, km_l)

def estimate_leg_liters(
      *
    , distance_km: float
    , cargo_t: float
    , spec: Dict[str, Any]
    , empty_backhaul_share: float = 0.0     # 0..1 of trips return empty
    , elasticity: float = 1.0
) -> Tuple[float, float, float, int, float, float]:
    """
    Returns:
      liters_total, liters_loaded, liters_empty, trips, kmL_loaded, kmL_empty
    Notes:
      • Trips are approximated as ceil(cargo_t / payload_t), assuming full loads.
      • Empty efficiency gain is taken from spec['empty_efficiency_gain'].
    """
    payload_t          = float(spec.get("payload_t", 27.0))
    axles              = int(spec.get("axles", 5))
    ref_weight_t       = float(spec.get("ref_weight_t", 20.0))
    empty_eff_gain     = float(spec.get("empty_efficiency_gain", 0.18))

    trips = max(1, math.ceil(float(cargo_t) / payload_t))

    kmL_base   = get_km_l_baseline(axles=axles)
    kmL_loaded = adjust_km_per_liter(
          km_l_baseline=kmL_base
        , cargo_weight_t=payload_t      # assume full load for planning
        , ref_weight_t=ref_weight_t
        , elasticity=elasticity
    )
    kmL_empty  = kmL_loaded * (1.0 + empty_eff_gain)

    liters_loaded = (distance_km / kmL_loaded) * trips
    liters_empty  = (distance_km / kmL_empty)  * (trips * float(empty_backhaul_share))
    liters_total  = liters_loaded + liters_empty

    return (
          float(liters_total)
        , float(liters_loaded)
        , float(liters_empty)
        , int(trips)
        , float(kmL_loaded)
        , float(kmL_empty)
    )
