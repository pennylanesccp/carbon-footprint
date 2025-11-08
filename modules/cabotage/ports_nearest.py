# modules/cabotage/ports_nearest.py
# Nearest-port utilities (gate-aware) with a simple haversine

from __future__ import annotations
from typing import Any as _Any, Dict as _Dict, List as _List, Optional as _Opt, Tuple as _T
import math

def haversine_km(
      lat1: float
    , lon1: float
    , lat2: float
    , lon2: float
) -> float:
    r = 6371.0088  # mean Earth radius (km)
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi   = math.radians(lat2 - lat1)
    dl     = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dl/2)**2
    return 2 * r * math.asin(math.sqrt(a))

def _best_port_anchor(
      port: _Dict[str, _Any]
    , lat: float
    , lon: float
) -> _T[float, float, _Opt[_Dict[str, _Any]]]:
    """
    Returns best (lat,lon) to measure distance to this port:
      - If gates exist, use the closest gate
      - Else, use port centroid
    Also returns the gate dict used (or None if centroid).
    """
    gates = port.get("gates") or []
    if gates:
        best = None
        best_d = float("inf")
        for g in gates:
            d = haversine_km(lat, lon, float(g["lat"]), float(g["lon"]))
            if d < best_d:
                best_d = d
                best   = g
        return float(best["lat"]), float(best["lon"]), best
    # fallback: centroid
    return float(port["lat"]), float(port["lon"]), None

def port_distance_km(
      lat: float
    , lon: float
    , port: _Dict[str, _Any]
) -> _T[float, _Opt[_Dict[str, _Any]]]:
    plat, plon, gate = _best_port_anchor(port, lat, lon)
    return haversine_km(lat, lon, plat, plon), gate

def find_nearest_port(
      lat: float
    , lon: float
    , ports: _List[_Dict[str, _Any]]
) -> _Dict[str, _Any]:
    """
    Returns:
      {
          "name", "city", "state", "lat", "lon",
        , "distance_km", "gate": {...} | None
      }
    """
    best = None
    best_d = float("inf")
    best_gate = None
    best_port = None

    for p in ports:
        d, gate = port_distance_km(lat, lon, p)
        if d < best_d:
            best_d    = d
            best_gate = gate
            best_port = p

    if best_port is None:
        raise ValueError("No ports provided.")

    return {
          "name":  best_port["name"]
        , "city":  best_port["city"]
        , "state": best_port["state"]
        , "lat":   best_port["lat"]
        , "lon":   best_port["lon"]
        , "distance_km": best_d
        , "gate": best_gate  # None if using centroid
    }
