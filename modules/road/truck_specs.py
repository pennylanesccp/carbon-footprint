# -*- coding: utf-8 -*-
"""
Truck presets for Brazil — payload, axle count, and reference weight for fuel calc.
These are *engineering presets* you control. Legal limits vary by config (PBTC/CMT);
keep those in a separate compliance check if you later need strict validation.
"""

from __future__ import annotations

from typing import Dict, Any

TRUCK_SPECS: Dict[str, Dict[str, Any]] = {
    # ────────────────────────────────────────────────────────────────────────────
    # Common container road-haul presets (names kept compatible with your code)
    # ────────────────────────────────────────────────────────────────────────────
      "semi_27t": {
          "label": "Carreta (5 eixos) ~27 t payload"
        , "axles": 5                   # for ANTT baseline km/L
        , "payload_t": 27.0            # engineering payload you intend to carry
        , "ref_weight_t": 20.0         # reference 'loaded' weight for baseline km/L
        , "empty_efficiency_gain": 0.18  # +18% km/L when return is empty
    }
    , "carreta_6ax_30t": {
          "label": "Carreta (6 eixos) ~30 t payload"
        , "axles": 6
        , "payload_t": 30.0
        , "ref_weight_t": 22.0
        , "empty_efficiency_gain": 0.18
    }
    , "bitrain_7ax_36t": {
          "label": "Bitrem (7 eixos) ~36 t payload"
        , "axles": 7
        , "payload_t": 36.0
        , "ref_weight_t": 24.0
        , "empty_efficiency_gain": 0.20
    }
    , "rodotrem_9ax_48t": {
          "label": "Rodotrem (9 eixos) ~48 t payload"
        , "axles": 9
        , "payload_t": 48.0
        , "ref_weight_t": 28.0
        , "empty_efficiency_gain": 0.22
    }
    , "auto_by_weight": {
          "label": "Auto (infer axles from payload)"
        , "axles": 5            # placeholder; overridden at runtime
        , "payload_t": 27.0     # placeholder
        , "ref_weight_t": 27.0  # placeholder
        , "empty_efficiency_gain": 0.18
    }
}

__all__ = ["TRUCK_SPECS"]
