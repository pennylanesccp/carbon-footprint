# modules/cabotage/ports_index.py
# Load container terminals (CTs) and find nearest by truck time.

from __future__ import annotations
import os, json, math
from typing import Any, Dict, List
from modules.road.ors_client import ORSClient

# Look for data/ports_index.json; if missing, use a tiny built-in fallback.
_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_FALLBACK_CTS = [
    { "code": "BRSSZ", "name": "Santos (SP)", "state": "SP", "lat": -23.9527, "lon": -46.3273, "thc_brl": 1300.0 },
    { "code": "BRRJO", "name": "Rio de Janeiro (RJ)", "state": "RJ", "lat": -22.9008, "lon": -43.1670, "thc_brl": 1300.0 },
    { "code": "BRPNG", "name": "Paranaguá (PR)", "state": "PR", "lat": -25.5149, "lon": -48.5091, "thc_brl": 1200.0 },
]

def load_cts() -> List[Dict[str, Any]]:
    path = os.path.abspath(os.path.join(_DATA_DIR, "ports_index.json"))
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return _FALLBACK_CTS

def _hav(lat1, lon1, lat2, lon2) -> float:
    R = 6371_000.0
    import math as m
    phi1, phi2 = m.radians(lat1), m.radians(lat2)
    dphi = phi2 - phi1; dl = m.radians(lon2 - lon1)
    a = m.sin(dphi/2)**2 + m.cos(phi1)*m.cos(phi2)*m.sin(dl/2)**2
    return 2*R*m.asin(m.sqrt(a))

def nearest_ct_by_hgv_time(point: Any, *, ors: ORSClient, max_candidates: int = 8) -> Dict[str, Any]:
    from modules.addressing.resolver import resolve_point
    p = resolve_point(point, ors=ors)
    cts = load_cts()
    cands = sorted(cts, key=lambda ct: _hav(p["lat"], p["lon"], ct["lat"], ct["lon"]))[:max_candidates]

    origins      = [p]
    destinations = [ { "lat": ct["lat"], "lon": ct["lon"], "label": ct["name"] } for ct in cands ]
    mx = ors.matrix_road(origins, destinations, profile="driving-hgv")
    best_j = min(range(len(cands)), key=lambda j: mx["durations_s"][0][j])
    best_ct = cands[best_j]
    return {
          "ct": best_ct
        , "duration_s": mx["durations_s"][0][best_j]
        , "distance_m": mx["distances_m"][0][best_j]
        , "origin": p
    }
