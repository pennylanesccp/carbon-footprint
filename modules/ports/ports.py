# modules/cabotage/router.py
# ────────────────────────────────────────────────────────────────────────────────
# Cabotage router
#   • Door→door: origin → origin_port → destination_port → destination
#   • Road legs via ORS client (distance_m / duration_s)
#   • Sea leg via SeaMatrix (preferred) or Haversine fallback
#   • Port-handling dwell (hours) + fixed cost at both ports
# ────────────────────────────────────────────────────────────────────────────────

from __future__ import annotations
from typing import Dict, Any, Tuple, Optional, List
from dataclasses import dataclass

from modules.addressing.resolver import resolve_point
from modules.cabotage import ports_index
from modules.ports.ports_nearest import find_nearest_port
from modules.cabotage.sea_matrix import SeaMatrix

import math

__all__ = [
      "DEFAULTS"
    , "find_cabotage_route"
    , "choose_nearest_ports"
    , "compute_port_to_port_sea_km"
]

# ─────────────────────────────── assumptions (override via **overrides)
DEFAULTS: Dict[str, float] = {
      "SEA_SPEED_KMH":       30.0
    , "SEA_COST_PER_KM":      0.60
    , "SEA_CO2_T_PER_KM":     0.00015
    , "ROAD_COST_PER_KM":     4.50
    , "ROAD_CO2_T_PER_KM":    0.00100
    , "PORT_HANDLING_COST": 800.00
    , "PORT_HANDLING_HOURS": 12.00
}

# ─────────────────────────────── internal datatypes (lightweight)
@dataclass
class Point:
    label: str
    lat: float
    lon: float

@dataclass
class Leg:
    mode: str
    origin: Dict[str, Any]
    destination: Dict[str, Any]
    distance_km: float
    hours: float
    cost_brl: float
    co2eq_t: float
    extras: Dict[str, Any]


# ─────────────────────────────── utils
def _haversine_km(
      lat1: float
    , lon1: float
    , lat2: float
    , lon2: float
) -> float:
    R = 6371.0088
    a1 = math.radians(lat1); b1 = math.radians(lon1)
    a2 = math.radians(lat2); b2 = math.radians(lon2)
    da = a2 - a1; db = b2 - b1
    s = (math.sin(da/2)**2
         + math.cos(a1) * math.cos(a2) * math.sin(db/2)**2)
    c = 2 * math.atan2(math.sqrt(s), math.sqrt(1 - s))
    return R * c


def _roadpoint_for_port(p: Dict) -> Point:
    """
    Prefer truck-gate coordinates; fallback to port centroid.
    Expected keys in *p*: name, lat, lon, gate_lat?, gate_lon?
    """
    lat = float(p.get("gate_lat", p["lat"]))
    lon = float(p.get("gate_lon", p["lon"]))
    return Point(
          label=str(p["name"])
        , lat=lat
        , lon=lon
    )


# ─────────────────────────────── leg builders
def _road_leg(
      o: Dict[str, Any]
    , d: Dict[str, Any]
    , *
    , ors
    , rate_brl_km: float
    , co2_t_km: float
) -> Leg:
    """
    Calls ORS wrapper: ors.route_road(origin_dict, destination_dict) and
    expects keys: distance_m, duration_s, origin, destination.
    """
    r = ors.route_road(o, d)  # your wrapper already handles snapping / geocode
    km = float((r.get("distance_m") or 0.0) / 1000.0)
    hr = float((r.get("duration_s") or 0.0) / 3600.0)

    return Leg(
          mode="road"
        , origin=r.get("origin", o)
        , destination=r.get("destination", d)
        , distance_km=km
        , hours=hr
        , cost_brl=km * float(rate_brl_km)
        , co2eq_t=km * float(co2_t_km)
        , extras={ "raw": {k: r.get(k) for k in ("distance_m", "duration_s")} }
    )


def _sea_leg(
      p_from: Dict[str, Any]
    , p_to: Dict[str, Any]
    , *
    , sea_speed_kmh: float
    , sea_cost_km: float
    , sea_co2_km: float
    , sea_matrix: Optional[SeaMatrix] = None
) -> Leg:
    """
    Sea leg distance:
      • Prefer SeaMatrix (name→name); if missing, Haversine fallback.
      • Uses port centroids for the fallback computation.
    """
    if sea_matrix:
        km = float(sea_matrix.get_km(
              p_from["name"]
            , p_to["name"]
            , (float(p_from["lat"]), float(p_from["lon"]))
            , (float(p_to["lat"]),   float(p_to["lon"]))
        ))
    else:
        km = _haversine_km(
              float(p_from["lat"]), float(p_from["lon"])
            , float(p_to["lat"]),   float(p_to["lon"])
        )

    hr = (km / float(sea_speed_kmh)) if sea_speed_kmh > 0 else 0.0

    return Leg(
          mode="sea"
        , origin={ "label": p_from["name"], "lat": p_from["lat"], "lon": p_from["lon"], "vessel": "container" }
        , destination={ "label": p_to["name"], "lat": p_to["lat"], "lon": p_to["lon"] }
        , distance_km=km
        , hours=hr
        , cost_brl=km * float(sea_cost_km)
        , co2eq_t=km * float(sea_co2_km)
        , extras={ "vessel": "container", "distance_source": ("matrix" if sea_matrix else "haversine") }
    )


# ─────────────────────────────── orchestrator
def choose_nearest_ports(
      origin_point: Dict[str, Any]
    , destination_point: Dict[str, Any]
    , *
    , all_ports: Optional[List[Dict[str, Any]]] = None
) -> Tuple[Dict, Dict]:
    """
    Pick the nearest cabotage ports to each endpoint (by truck-gate if present).
    Returns two *port dicts* as provided by ports_index.load_ports().
    """
    P = all_ports or ports_index.load_ports()
    p_o, _ = find_nearest_port(
          float(origin_point["lat"])
        , float(origin_point["lon"])
        , P
        , use_gate=True
    )
    p_d, _ = find_nearest_port(
          float(destination_point["lat"])
        , float(destination_point["lon"])
        , P
        , use_gate=True
    )
    return p_o, p_d


def compute_port_to_port_sea_km(
      port_from: Dict[str, Any]
    , port_to: Dict[str, Any]
    , *
    , sea_matrix: Optional[SeaMatrix] = None
) -> float:
    """
    Convenience: get sea distance (km) for two *port dicts* (from ports_index).
    """
    if sea_matrix:
        return float(sea_matrix.get_km(
              port_from["name"]
            , port_to["name"]
            , (float(port_from["lat"]), float(port_from["lon"]))
            , (float(port_to["lat"]),   float(port_to["lon"]))
        ))
    return _haversine_km(
          float(port_from["lat"]), float(port_from["lon"])
        , float(port_to["lat"]),   float(port_to["lon"])
    )


def find_cabotage_route(
      origin
    , destination
    , *
    , ors
    , sea_matrix: Optional[SeaMatrix] = None
    , **overrides
) -> Dict[str, Any]:
    """
    Compute door→door route through cabotage:
      origin → origin_port → destination_port → destination

    Parameters
    ----------
    origin, destination : any
        Resolved via `resolve_point()`; can be address strings, (lat,lon), etc.
    ors : object
        Your ORS wrapper providing `route_road(origin_dict, destination_dict)`.
    sea_matrix : SeaMatrix | None
        Preloaded sea-distance matrix (km). If None, fallback is Haversine.

    Overrides (optional)
    --------------------
    SEA_SPEED_KMH, SEA_COST_PER_KM, SEA_CO2_T_PER_KM,
    ROAD_COST_PER_KM, ROAD_CO2_T_PER_KM,
    PORT_HANDLING_COST, PORT_HANDLING_HOURS
    """
    # 0) Resolve endpoints
    o = resolve_point(origin, ors=ors)
    d = resolve_point(destination, ors=ors)

    # 1) Load ports & choose nearest by gate
    P = ports_index.load_ports()
    p_o, p_d = choose_nearest_ports(o, d, all_ports=P)

    # 2) Effective road points for ORS
    rp_o = _roadpoint_for_port(p_o)
    rp_d = _roadpoint_for_port(p_d)

    # 3) Assumptions
    A = {**DEFAULTS, **overrides}

    # 4) Legs
    leg1 = _road_leg(
          { "label": o.get("label", "origin"), "lat": float(o["lat"]), "lon": float(o["lon"]) }
        , { "label": rp_o.label,               "lat": rp_o.lat,        "lon": rp_o.lon }
        , ors=ors
        , rate_brl_km=A["ROAD_COST_PER_KM"]
        , co2_t_km=A["ROAD_CO2_T_PER_KM"]
    )

    sea = _sea_leg(
          { "name": p_o["name"], "lat": float(p_o["lat"]), "lon": float(p_o["lon"]) }
        , { "name": p_d["name"], "lat": float(p_d["lat"]), "lon": float(p_d["lon"]) }
        , sea_speed_kmh=A["SEA_SPEED_KMH"]
        , sea_cost_km=A["SEA_COST_PER_KM"]
        , sea_co2_km=A["SEA_CO2_T_PER_KM"]
        , sea_matrix=sea_matrix
    )
    # Add handling (both ports)
    sea.hours    += 2.0 * float(A["PORT_HANDLING_HOURS"])
    sea.cost_brl += 2.0 * float(A["PORT_HANDLING_COST"])
    sea.extras[ "port_handling_hours" ] = float(A["PORT_HANDLING_HOURS"]) * 2.0
    sea.extras[ "port_handling_cost"  ] = float(A["PORT_HANDLING_COST"])  * 2.0

    leg3 = _road_leg(
          { "label": rp_d.label,               "lat": rp_d.lat,        "lon": rp_d.lon }
        , { "label": d.get("label", "destination"), "lat": float(d["lat"]), "lon": float(d["lon"]) }
        , ors=ors
        , rate_brl_km=A["ROAD_COST_PER_KM"]
        , co2_t_km=A["ROAD_CO2_T_PER_KM"]
    )

    legs: List[Leg] = [leg1, sea, leg3]

    # 5) Totals
    total_distance = sum(l.distance_km for l in legs)
    total_hours    = sum(l.hours       for l in legs)
    total_cost     = sum(l.cost_brl    for l in legs)
    total_co2      = sum(l.co2eq_t     for l in legs)

    # 6) Pack output (dicts for easy JSON)
    legs_out = [{
          "mode":        l.mode
        , "origin":      l.origin
        , "destination": l.destination
        , "distance_km": l.distance_km
        , "hours":       l.hours
        , "cost_brl":    l.cost_brl
        , "co2eq_t":     l.co2eq_t
        , "extras":      l.extras
    } for l in legs]

    return {
          "origin": {
              "label": o.get("label")
            , "lat":   float(o["lat"])
            , "lon":   float(o["lon"])
        }
        , "destination": {
              "label": d.get("label")
            , "lat":   float(d["lat"])
            , "lon":   float(d["lon"])
        }
        , "ports_used": [
              { "name": p_o["name"], "lat": float(p_o["lat"]), "lon": float(p_o["lon"]) }
            , { "name": p_d["name"], "lat": float(p_d["lat"]), "lon": float(p_d["lon"]) }
        ]
        , "legs": legs_out
        , "totals": {
              "distance_km": total_distance
            , "hours":       total_hours
            , "cost_brl":    total_cost
            , "co2eq_t":     total_co2
        }
        , "assumptions": A
    }
