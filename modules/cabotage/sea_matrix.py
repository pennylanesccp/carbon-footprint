# modules/cabotage/sea_matrix.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Tuple, Optional
import json
import math
from pathlib import Path

__all__ = ["SeaMatrix"]

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0088
    a1 = math.radians(lat1); b1 = math.radians(lon1)
    a2 = math.radians(lat2); b2 = math.radians(lon2)
    da = a2 - a1; db = b2 - b1
    s  = (math.sin(da/2)**2
          + math.cos(a1) * math.cos(a2) * math.sin(db/2)**2)
    c  = 2 * math.atan2(math.sqrt(s), math.sqrt(1 - s))
    return R * c

def _norm(label: str) -> str:
    # simple normalization (casefold + strip + single spaces)
    return " ".join(str(label).casefold().split())

@dataclass
class SeaMatrix:
    """
    Minimal sea-distance matrix:
      • matrix[label_from][label_to] -> km (float)
      • coastline_factor: multiplier for haversine fallback
    """
    matrix: Dict[str, Dict[str, float]]
    coastline_factor: float = 1.0

    def __post_init__(self):
        # Build normalization map to keep canonical labels
        self._canon: Dict[str, str] = {}
        for r in self.matrix.keys():
            self._canon.setdefault(_norm(r), r)
            for c in self.matrix[r].keys():
                self._canon.setdefault(_norm(c), c)
        # Ensure symmetry (A→B implies B→A)
        for r, cols in list(self.matrix.items()):
            for c, km in list(cols.items()):
                self.matrix.setdefault(c, {})
                if c not in self.matrix or r not in self.matrix[c]:
                    self.matrix[c][r] = float(km)

    # ---------- constructors ----------
    @classmethod
    def from_json_dict(
        cls
        , payload: Dict
    ) -> "SeaMatrix":
        m = payload.get("matrix") or {}
        cf = float(payload.get("coastline_factor", 1.0))
        # cast to float
        m_float = {
            r: {c: float(v) for c, v in cols.items()}
            for r, cols in m.items()
        }
        return cls(matrix=m_float, coastline_factor=cf)

    @classmethod
    def from_json_path(
        cls
        , path: Path | str
    ) -> "SeaMatrix":
        p = Path(path)
        with p.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return cls.from_json_dict(payload)

    # ---------- queries ----------
    def size(self) -> int:
        return len(self._canon)

    def labels(self) -> Tuple[str, ...]:
        return tuple(sorted(self._canon.values()))

    def _resolve_label(self, label: str) -> Optional[str]:
        return self._canon.get(_norm(label))

    def get(
        self
        , a_label: str
        , b_label: str
    ) -> Optional[float]:
        a = self._resolve_label(a_label)
        b = self._resolve_label(b_label)
        if not a or not b:
            return None
        if a == b:
            return 0.0
        return self.matrix.get(a, {}).get(b)

    def km_with_source(
        self
        , p_from: Dict
        , p_to: Dict
    ) -> Tuple[float, str]:
        """Return (km, source) where source ∈ {'matrix','haversine'}."""
        a_label = p_from["name"]
        b_label = p_to["name"]
        km = self.get(a_label, b_label)
        if km is not None:
            return float(km), "matrix"

        # fallback: haversine * coastline factor
        km_h = _haversine_km(
              float(p_from["lat"])
            , float(p_from["lon"])
            , float(p_to["lat"])
            , float(p_to["lon"])
        )
        return km_h * float(self.coastline_factor), "haversine"

    def km(
        self
        , p_from: Dict
        , p_to: Dict
    ) -> float:
        km, _ = self.km_with_source(p_from, p_to)
        return km
