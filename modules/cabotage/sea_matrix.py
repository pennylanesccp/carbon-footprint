# modules/cabotage/sea_matrix.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Sea distance matrix (with haversine fallback)
=============================================

Purpose
-------
Provide deterministic **port-to-port** sea distances (km) from a prebuilt matrix,
with a safe fallback to **haversine distance × coastline_factor** when a pair is
not explicitly listed.

Public API (kept stable)
------------------------
- class SeaMatrix
  • constructors:
      - SeaMatrix.from_json_dict(payload: Dict) -> SeaMatrix
      - SeaMatrix.from_json_path(path: Path | str) -> SeaMatrix
  • queries:
      - size() -> int
      - labels() -> Tuple[str, ...]
      - get(a_label: str, b_label: str) -> Optional[float]
      - km_with_source(p_from: Dict, p_to: Dict) -> Tuple[float, str]   # source ∈ {'matrix','haversine'}
      - km(p_from: Dict, p_to: Dict) -> float

Conventions
-----------
- Labels are normalized (casefold + trim + single spaces) internally for robust lookup.
- The internal matrix is kept **symmetric**: if A→B exists, B→A is ensured at init.
- Inputs to km/km_with_source expect dicts with keys: 'name', 'lat', 'lon'.

Logging
-------
Uses the project-standard logger. Construction and lookups are logged at INFO/DEBUG.
"""

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Optional, Any

from modules.infra.logging import get_logger

__all__ = ["SeaMatrix"]

_log = get_logger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _haversine_km(
    lat1: float
    , lon1: float
    , lat2: float
    , lon2: float
) -> float:
    """
    Great-circle distance on WGS84 sphere approximation (km).
    """
    R = 6371.0088  # mean Earth radius (km)
    a1 = math.radians(float(lat1))
    b1 = math.radians(float(lon1))
    a2 = math.radians(float(lat2))
    b2 = math.radians(float(lon2))
    da = a2 - a1
    db = b2 - b1
    s = (
          math.sin(da / 2) ** 2
        + math.cos(a1) * math.cos(a2) * math.sin(db / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(s), math.sqrt(1 - s))
    return float(R * c)


def _norm(
    label: str
) -> str:
    """
    Simple normalization (casefold, trim, collapse spaces).
    """
    return " ".join(str(label).casefold().split())


# ────────────────────────────────────────────────────────────────────────────────
# Core class
# ────────────────────────────────────────────────────────────────────────────────
@dataclass
class SeaMatrix:
    """
    Minimal sea-distance matrix.

    Attributes
    ----------
    matrix : Dict[str, Dict[str, float]]
        Mapping 'label_from' → {'label_to': km}
    coastline_factor : float
        Multiplier applied to haversine fallback to roughly account for coastline routing.
    """
    matrix: Dict[str, Dict[str, float]]
    coastline_factor: float = 1.0

    # Internal: normalized → canonical label map
    _canon: Dict[str, str] = None  # type: ignore

    def __post_init__(self) -> None:
        # 1) Ensure values are floats and keys are strings
        cleaned: Dict[str, Dict[str, float]] = {}
        for r, cols in (self.matrix or {}).items():
            r_str = str(r)
            cleaned[r_str] = {}
            for c, km in (cols or {}).items():
                cleaned[r_str][str(c)] = float(km)

        self.matrix = cleaned
        self.coastline_factor = float(self.coastline_factor)

        # 2) Build normalization map (first occurrence becomes canonical)
        self._canon = {}
        for r in self.matrix.keys():
            self._canon.setdefault(_norm(r), r)
            for c in self.matrix[r].keys():
                self._canon.setdefault(_norm(c), c)

        # 3) Enforce symmetry (A→B implies B→A)
        for r, cols in list(self.matrix.items()):
            for c, km in list(cols.items()):
                self.matrix.setdefault(c, {})
                if r not in self.matrix[c]:
                    self.matrix[c][r] = float(km)

        # 4) Log summary
        n_labels = len(self._canon)
        n_pairs = sum(len(v) for v in self.matrix.values())
        _log.info(
              "SeaMatrix: initialized with %d labels, %d directed edges (symmetric), "
              "coastline_factor=%.3f"
            , n_labels
            , n_pairs
            , self.coastline_factor
        )

    # ---------- constructors ----------
    @classmethod
    def from_json_dict(
        cls
        , payload: Dict[str, Any]
    ) -> "SeaMatrix":
        """
        Build from a JSON-like dict:

            {
              "matrix": {"A": {"B": 123.4}, ...},
              "coastline_factor": 1.12
            }
        """
        if not isinstance(payload, dict):
            raise TypeError("SeaMatrix.from_json_dict: payload must be a dict.")

        m = payload.get("matrix") or {}
        cf = float(payload.get("coastline_factor", 1.0))

        # Cast to the expected types
        m_float: Dict[str, Dict[str, float]] = {
            str(r): {str(c): float(v) for c, v in (cols or {}).items()}
            for r, cols in (m or {}).items()
        }
        _log.debug(
              "SeaMatrix.from_json_dict: coastline_factor=%.3f, nodes=%d"
            , cf
            , len(m_float)
        )
        return cls(matrix=m_float, coastline_factor=cf)

    @classmethod
    def from_json_path(
        cls
        , path: Path | str
    ) -> "SeaMatrix":
        """
        Build from a JSON file on disk.
        """
        p = Path(path)
        with p.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        _log.info("SeaMatrix.from_json_path: loaded JSON from '%s'.", p)
        return cls.from_json_dict(payload)

    # ---------- queries ----------
    def size(self) -> int:
        """
        Number of distinct canonical labels.
        """
        return len(self._canon)

    def labels(self) -> Tuple[str, ...]:
        """
        Sorted tuple of canonical labels.
        """
        return tuple(sorted(self._canon.values()))

    def _resolve_label(
        self
        , label: Optional[str]
    ) -> Optional[str]:
        """
        Return canonical label for a possibly messy label; None if unknown.
        """
        if label is None:
            return None
        return self._canon.get(_norm(label))

    def get(
        self
        , a_label: str
        , b_label: str
    ) -> Optional[float]:
        """
        Direct matrix lookup (no fallback).

        Returns
        -------
        Optional[float]
            km if known; 0.0 if same label; None if either label is unknown
            or pair not present in matrix.
        """
        a = self._resolve_label(a_label)
        b = self._resolve_label(b_label)

        if not a or not b:
            _log.debug(
                  "SeaMatrix.get: unknown label(s) a=%r, b=%r."
                , a_label
                , b_label
            )
            return None

        if a == b:
            return 0.0

        val = self.matrix.get(a, {}).get(b)
        _log.debug(
              "SeaMatrix.get: a=%r b=%r → %s"
            , a
            , b
            , val if val is not None else "None"
        )
        return None if val is None else float(val)

    def km_with_source(
        self
        , p_from: Dict[str, Any]
        , p_to: Dict[str, Any]
    ) -> Tuple[float, str]:
        """
        Compute distance with source information.

        Parameters
        ----------
        p_from, p_to : Dict
            Expect keys: 'name', 'lat', 'lon'

        Returns
        -------
        (km, source)
            source ∈ {'matrix','haversine'}
        """
        a_label = p_from["name"]
        b_label = p_to["name"]

        km = self.get(a_label, b_label)
        if km is not None:
            _log.info(
                  "SeaMatrix.km_with_source: using MATRIX for %r → %r → %.3f km"
                , a_label
                , b_label
                , km
            )
            return float(km), "matrix"

        # Fallback: haversine × coastline_factor
        lat1 = float(p_from["lat"])
        lon1 = float(p_from["lon"])
        lat2 = float(p_to["lat"])
        lon2 = float(p_to["lon"])

        km_h = _haversine_km(lat1, lon1, lat2, lon2)
        km_c = km_h * float(self.coastline_factor)

        _log.info(
              "SeaMatrix.km_with_source: HAVERSINE fallback for %r → %r "
              "(haversine=%.3f km, coastline_factor=%.3f → %.3f km)"
            , a_label
            , b_label
            , km_h
            , self.coastline_factor
            , km_c
        )
        return float(km_c), "haversine"

    def km(
        self
        , p_from: Dict[str, Any]
        , p_to: Dict[str, Any]
    ) -> float:
        """
        Distance only (km).
        """
        km_val, _ = self.km_with_source(p_from, p_to)
        return float(km_val)


# ────────────────────────────────────────────────────────────────────────────────
# CLI / direct smoke test
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Quick logging smoke test:

        python -m modules.cabotage.sea_matrix
    """
    from modules.infra.logging import init_logging

    init_logging(level="INFO", force=True, write_output=False)

    payload = {
        "matrix": {
            "Santos (SP)": {
                "Rio de Janeiro (RJ)": 430.0
            }
        },
        "coastline_factor": 1.15,
    }

    sm = SeaMatrix.from_json_dict(payload)

    a = {
          "name": "Santos (SP)"
        , "lat": -23.952
        , "lon": -46.328
    }
    b = {
          "name": "Rio de Janeiro (RJ)"
        , "lat": -22.903
        , "lon": -43.172
    }
    c = {
          "name": "Itajaí"
        , "lat": -26.904
        , "lon": -48.659
    }

    print("size=", sm.size())
    print("labels=", sm.labels())
    print("matrix_km=", sm.km(a, b))
    print("fallback_km=", sm.km(c, a))
