# modules/fuel/truck_specs.py
# -*- coding: utf-8 -*-
"""
Truck presets for Brazil — payload, axle count, and reference weight for fuel calc.

This module centralizes *engineering presets* that your model uses to:
- pick a representative axle count (for baseline ANTT km/L),
- set an intended payload (t),
- pick a reference 'loaded' weight used by your fuel model,
- adjust efficiency for empty backhaul legs.

Notes
-----
• Legal limits (PBTC/CMT) vary by configuration and fuel class — **do not** treat
  these presets as compliance validation. Keep regulatory checks elsewhere.
• The 'auto_by_weight' option is resolved at runtime by your fuel layer; helpers
  here expose default heuristics you can reuse if needed.
"""

from __future__ import annotations

from typing import Dict, Any, Optional, List
from copy import deepcopy

from modules.infra.logging import get_logger

_log = get_logger(__name__)

# ────────────────────────────────────────────────────────────────────────────────
# ANTT baseline efficiency (km/L) by axle-count
# Source: user-provided table (official ANTT reference)
# ────────────────────────────────────────────────────────────────────────────────
ANTT_KM_PER_L_BY_AXLES: Dict[int, float] = {
    2: 4.0,
    3: 3.0,
    4: 2.7,
    5: 2.3,
    6: 2.0,
    7: 2.0,   # ≥7 axles commonly treated with the same baseline (see note below)
    # 9+ handled by fallback in baseline_km_per_l_from_axles()
}

# ────────────────────────────────────────────────────────────────────────────────
# Presets
# ────────────────────────────────────────────────────────────────────────────────
TRUCK_SPECS: Dict[str, Dict[str, Any]] = {
    # Common container fuel-haul presets (keys kept compatible with repo)
    "semi_27t": {
        "label": "Carreta (5 eixos) ~27 t payload",
        "axles": 5,                    # used for ANTT baseline km/L
        "payload_t": 27.0,             # engineering payload you intend to carry
        "ref_weight_t": 20.0,          # 'loaded' reference for fuel model
        "empty_efficiency_gain": 0.18, # +18% km/L when return is empty
    },
    "carreta_6ax_30t": {
        "label": "Carreta (6 eixos) ~30 t payload",
        "axles": 6,
        "payload_t": 30.0,
        "ref_weight_t": 22.0,
        "empty_efficiency_gain": 0.18,
    },
    "bitrain_7ax_36t": {
        "label": "Bitrem (7 eixos) ~36 t payload",
        "axles": 7,
        "payload_t": 36.0,
        "ref_weight_t": 24.0,
        "empty_efficiency_gain": 0.20,
    },
    "rodotrem_9ax_48t": {
        "label": "Rodotrem (9 eixos) ~48 t payload",
        "axles": 9,
        "payload_t": 48.0,
        "ref_weight_t": 28.0,
        "empty_efficiency_gain": 0.22,
    },
    "auto_by_weight": {
        "label": "Auto (infer axles from payload)",
        "axles": 5,             # placeholder; override at runtime
        "payload_t": 27.0,      # placeholder; your evaluator/fuel layer sets this
        "ref_weight_t": 27.0,   # placeholder; updated alongside the axle guess
        "empty_efficiency_gain": 0.18,
    },
}

__all__ = [
    "TRUCK_SPECS",
    "ANTT_KM_PER_L_BY_AXLES",
    "list_truck_keys",
    "get_truck_spec",
    "guess_axles_from_payload",
    "baseline_km_per_l_from_axles",
]

# ────────────────────────────────────────────────────────────────────────────────
# Tiny helper API (pure, reusable, well-logged)
# ────────────────────────────────────────────────────────────────────────────────

def list_truck_keys() -> List[str]:
    """
    Return available preset keys (stable order for CLI help/UX).
    """
    keys = list(TRUCK_SPECS.keys())
    _log.debug("truck_specs: listing keys -> %s", keys)
    return keys


def get_truck_spec(truck_key: str) -> Dict[str, Any]:
    """
    Return a defensive copy of the preset so callers can tweak fields safely.
    Raises KeyError if unknown key.
    """
    if truck_key not in TRUCK_SPECS:
        _log.error("truck_specs: unknown truck_key=%s", truck_key)
        raise KeyError(f"Unknown truck preset: {truck_key}")
    spec = deepcopy(TRUCK_SPECS[truck_key])
    _log.debug(
        "truck_specs: get %s -> %s",
        truck_key,
        {k: spec[k] for k in ["label", "axles", "payload_t", "ref_weight_t"]},
    )
    return spec


def guess_axles_from_payload(payload_t: float) -> int:
    """
    Lightweight heuristic to infer axle count from intended *payload* (t).
    Tuned for long-haul container flows in BR; adjust if your domain changes.

        ≤18 t  -> 5 axles (carreta)
        18–32 t -> 6–7 axles (we bias to 6 up to ~30, then 7)
        >32 t  -> 9 axles (rodotrem-ish domain)

    Returns an integer axle count suitable for ANTT baseline lookup.
    """
    p = float(payload_t)
    if p <= 18.0:
        a = 5
    elif p <= 30.0:
        a = 6
    elif p <= 40.0:
        a = 7
    else:
        a = 9
    _log.debug("truck_specs: infer axles from payload_t=%.2f -> %s axles", p, a)
    return a


def baseline_km_per_l_from_axles(axles: int) -> float:
    """
    Return baseline ANTT km/L for the given axle count.
    For ≥8 axles, fallback to 2.0 km/L (conservative), unless you extend the table.
    """
    a = int(axles)
    if a in ANTT_KM_PER_L_BY_AXLES:
        kmpl = ANTT_KM_PER_L_BY_AXLES[a]
    elif a >= 8:
        kmpl = 2.0
    else:
        # For <2 or other unexpected values, be conservative but log loudly.
        _log.warning("truck_specs: unexpected axles=%s; using conservative 2.0 km/L", a)
        kmpl = 2.0
    _log.debug("truck_specs: baseline km/L for axles=%s -> %.3f", a, kmpl)
    return float(kmpl)


# ────────────────────────────────────────────────────────────────────────────────
# CLI / smoke test
# ────────────────────────────────────────────────────────────────────────────────

def main(argv: List[str] | None = None) -> int:
    """
    Small CLI / smoke test for truck presets and axle/efficiency helpers.

    Examples
    --------
    python -m modules.fuel.truck_specs
    python -m modules.fuel.truck_specs --truck-key bitrain_7ax_36t
    python -m modules.fuel.truck_specs --truck-key auto_by_weight --payload-t 34
    """
    import argparse
    import json

    from modules.infra.logging import init_logging

    parser = argparse.ArgumentParser(
        description=(
            "Truck presets (BR) — inspect a preset and derived axle / km/L."
        )
    )
    parser.add_argument(
          "--truck-key"
        , default="semi_27t"
        , choices=list_truck_keys()
        , help="Truck preset key to inspect."
    )
    parser.add_argument(
          "--payload-t"
        , type=float
        , default=None
        , help="Override payload_t (t). If omitted, uses preset payload_t."
    )
    parser.add_argument(
          "--axles"
        , type=int
        , default=None
        , help="Override axle count. If omitted, inferred from payload."
    )
    parser.add_argument(
          "--log-level"
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

    spec = get_truck_spec(args.truck_key)

    effective_payload = (
        float(args.payload_t)
        if args.payload_t is not None
        else float(spec["payload_t"])
    )
    effective_axles = (
        int(args.axles)
        if args.axles is not None
        else guess_axles_from_payload(effective_payload)
    )
    baseline_kmpl = baseline_km_per_l_from_axles(effective_axles)

    payload = {
          "truck_key": args.truck_key
        , "label": spec["label"]
        , "preset_axles": spec["axles"]
        , "preset_payload_t": spec["payload_t"]
        , "preset_ref_weight_t": spec["ref_weight_t"]
        , "preset_empty_efficiency_gain": spec["empty_efficiency_gain"]
        , "effective_payload_t": effective_payload
        , "effective_axles": effective_axles
        , "baseline_km_per_l": baseline_kmpl
    }

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
