# modules/cabotage/ports_nearest.py
# -*- coding: utf-8 -*-
"""
Nearest-port utilities (gate-aware) with haversine
==================================================

Purpose
-------
Given a latitude/longitude, find the **nearest port** from a normalized list
(see modules.cabotage.ports_index.load_ports). If a port has **gates**, the
distance is measured to the **closest gate**; otherwise to the port centroid.

Public API (kept stable)
------------------------
- haversine_km(lat1, lon1, lat2, lon2) -> float
- port_distance_km(lat, lon, port) -> Tuple[float, Optional[Dict[str, Any]]]
- find_nearest_port(lat, lon, ports) -> Dict[str, Any]
    returns:
    {
        "name", "city", "state", "lat", "lon",
        "distance_km", "gate": {...} | None
    }

Notes
-----
- Inputs should be numeric (floats). Basic coercions are applied and errors raise.
- Invalid gate entries are ignored; if none valid, centroid is used.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

from modules.functions._logging import get_logger

_log = get_logger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _to_float(x: Any, *, name: str) -> float:
    """Strict float coercion with explicit error context."""
    try:
        return float(x)
    except Exception as e:
        raise TypeError(f"Expected float-like value for '{name}', got {x!r}") from e


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Great-circle distance on a WGS84 sphere approximation (km).
    """
    lat1 = _to_float(lat1, name="lat1")
    lon1 = _to_float(lon1, name="lon1")
    lat2 = _to_float(lat2, name="lat2")
    lon2 = _to_float(lon2, name="lon2")

    r = 6371.0088  # mean Earth radius (km)
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    km = 2 * r * math.asin(min(1.0, math.sqrt(a)))  # guard for rounding
    return float(km)


def _best_port_anchor(port: Dict[str, Any], lat: float, lon: float) -> Tuple[float, float, Optional[Dict[str, Any]]]:
    """
    Choose the best (lat, lon) anchor for distance to *port*:

      - If gates exist, pick the closest valid gate to (lat, lon).
      - Else, return the port centroid.

    Returns
    -------
    (plat, plon, gate_dict_or_None)
    """
    lat = _to_float(lat, name="lat")
    lon = _to_float(lon, name="lon")

    gates = port.get("gates") or []
    best_gate: Optional[Dict[str, Any]] = None
    best_d = float("inf")
    if isinstance(gates, (list, tuple)) and len(gates) > 0:
        for g in gates:
            try:
                glat = _to_float(g.get("lat"), name="gate.lat")
                glon = _to_float(g.get("lon"), name="gate.lon")
            except Exception:
                # Skip invalid gate silently; debug-logged to avoid noise at INFO
                _log.debug("Skipping invalid gate in _best_port_anchor: %r", g)
                continue
            d = haversine_km(lat, lon, glat, glon)
            if d < best_d:
                best_d = d
                best_gate = {"label": g.get("label") or "gate", "lat": glat, "lon": glon}

        if best_gate is not None:
            _log.debug(
                "_best_port_anchor: using gate '%s' for port '%s' (d=%.3f km).",
                best_gate.get("label"), port.get("name"), best_d
            )
            return float(best_gate["lat"]), float(best_gate["lon"]), best_gate

    # Fallback: centroid
    plat = _to_float(port.get("lat"), name="port.lat")
    plon = _to_float(port.get("lon"), name="port.lon")
    _log.debug("_best_port_anchor: using centroid for port '%s'.", port.get("name"))
    return float(plat), float(plon), None


# ────────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────────
def port_distance_km(lat: float, lon: float, port: Dict[str, Any]) -> Tuple[float, Optional[Dict[str, Any]]]:
    """
    Distance (km) from (lat, lon) to the **best anchor** for *port* and the anchor meta.

    Returns
    -------
    (distance_km, gate_dict_or_None)
    """
    plat, plon, gate = _best_port_anchor(port, lat, lon)
    d = haversine_km(lat, lon, plat, plon)
    _log.debug(
        "port_distance_km: to port '%s' via %s → %.3f km",
        port.get("name"),
        f"gate '{gate.get('label')}'" if gate else "centroid",
        d,
    )
    return float(d), gate


def find_nearest_port(lat: float, lon: float, ports: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Find the nearest port to (lat, lon).

    Parameters
    ----------
    lat, lon : float
        Query coordinates.
    ports : List[Dict[str, Any]]
        Normalized port list (see modules.cabotage.ports_index.load_ports).

    Returns
    -------
    Dict[str, Any]
        {
            "name", "city", "state", "lat", "lon",
            "distance_km", "gate": {...} | None
        }

    Raises
    ------
    ValueError
        If *ports* is empty.
    """
    lat = _to_float(lat, name="lat")
    lon = _to_float(lon, name="lon")

    if not isinstance(ports, list) or len(ports) == 0:
        _log.error("find_nearest_port: empty or invalid 'ports' list.")
        raise ValueError("No ports provided.")

    best_port: Optional[Dict[str, Any]] = None
    best_gate: Optional[Dict[str, Any]] = None
    best_d = float("inf")

    for p in ports:
        try:
            d, gate = port_distance_km(lat, lon, p)
        except Exception as e:
            _log.debug("Skipping port due to error: %r (err=%s)", p, e)
            continue
        if d < best_d:
            best_d = d
            best_port = p
            best_gate = gate

    if best_port is None:
        _log.error("find_nearest_port: no valid ports after filtering.")
        raise ValueError("No valid ports after filtering invalid entries.")

    result = {
        "name": best_port["name"],
        "city": best_port["city"],
        "state": best_port["state"],
        "lat": float(best_port["lat"]),
        "lon": float(best_port["lon"]),
        "distance_km": float(best_d),
        "gate": best_gate,  # None if centroid
    }

    _log.info(
        "find_nearest_port: nearest='%s' (UF=%s) distance=%.3f km via %s.",
        result["name"],
        result["state"],
        result["distance_km"],
        f"gate '{best_gate.get('label')}'" if best_gate else "centroid",
    )
    return result


"""
Quick logging smoke test (PowerShell)
python -c `
"from modules.functions.logging import init_logging; `
from modules.cabotage.ports_nearest import haversine_km, port_distance_km, find_nearest_port; `
init_logging(level='INFO', force=True, write_output=False); `
ports = [ `
  { 'name':'Santos (SP)', 'city':'Santos', 'state':'SP', 'lat':-23.952, 'lon':-46.328, `
    'gates':[{'label':'Ponta da Praia','lat':-23.986,'lon':-46.296}, {'lat':-23.97,'lon':-46.33}] }, `
  { 'name':'Rio de Janeiro (RJ)', 'city':'Rio de Janeiro', 'state':'RJ', 'lat':-22.903, 'lon':-43.172 } `
]; `
print('haversine SP-RJ ~', round(haversine_km(-23.55,-46.63,-22.90,-43.17), 1), 'km'); `
print('dist→Santos:', port_distance_km(-23.55, -46.63, ports[0])); `
print('nearest:', find_nearest_port(-23.55, -46.63, ports)); "
"""
