# modules/cabotage/router.py
from __future__ import annotations
from typing import Dict, Any, Tuple
from modules.addressing.resolver import resolve_point
from modules.cabotage import ports_index
from modules.cabotage.ports_nearest import find_nearest_port

# cost/CO2 assumptions (override via kwargs if you want)
DEFAULTS = {
    "SEA_SPEED_KMH":      30.0,
    "SEA_COST_PER_KM":     0.6,
    "SEA_CO2_T_PER_KM":    0.00015,
    "ROAD_COST_PER_KM":    4.5,
    "ROAD_CO2_T_PER_KM":   0.001,
    "PORT_HANDLING_COST":  800.0,
    "PORT_HANDLING_HOURS": 12.0,
}

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math
    R = 6371.0088
    a1 = math.radians(lat1); b1 = math.radians(lon1)
    a2 = math.radians(lat2); b2 = math.radians(lon2)
    da = a2 - a1; db = b2 - b1
    s = (math.sin(da/2)**2
         + math.cos(a1) * math.cos(a2) * math.sin(db/2)**2)
    c = 2 * math.atan2(math.sqrt(s), math.sqrt(1 - s))
    return R * c

def _roadpoint_for_port(p: Dict) -> Dict:
    """Return the truck gate point for road routing (falls back to centroid)."""
    return {
          "lat": float(p.get("gate_lat", p["lat"]))
        , "lon": float(p.get("gate_lon", p["lon"]))
        , "label": p["name"]
    }

def _road_leg(o, d, *, ors, rate_brl_km: float, co2_t_km: float) -> Dict:
    r = ors.route_road(o, d)   # ORS client handles SNAP-on-404
    km = (r["distance_m"] or 0.0) / 1000.0
    hr = (r["duration_s"] or 0.0) / 3600.0
    return {
          "mode": "road"
        , "origin": r["origin"]
        , "destination": r["destination"]
        , "distance_km": km
        , "hours": hr
        , "cost_brl": km * rate_brl_km
        , "co2eq_t": km * co2_t_km
        , "_raw": {k: r[k] for k in ("origin", "destination", "distance_m", "duration_s")}
    }

def _sea_leg(p_from: Dict, p_to: Dict, *, sea_speed_kmh: float, sea_cost_km: float, sea_co2_km: float) -> Dict:
    # Sea leg uses port centroids for the nautical distance approximation
    km = _haversine_km(p_from["lat"], p_from["lon"], p_to["lat"], p_to["lon"])
    hr = km / sea_speed_kmh if sea_speed_kmh > 0 else 0.0
    return {
          "mode": "sea"
        , "origin": {"label": p_from["name"], "lat": p_from["lat"], "lon": p_from["lon"], "vessel": "container"}
        , "destination": {"label": p_to["name"], "lat": p_to["lat"], "lon": p_to["lon"]}
        , "distance_km": km
        , "hours": hr
        , "cost_brl": km * sea_cost_km
        , "co2eq_t": km * sea_co2_km
        , "vessel": "container"
    }

def find_cabotage_route(
      origin
    , destination
    , *
    , ors
    , **overrides
) -> Dict[str, Any]:
    """Door→door cabotage: origin → origin_port → destination_port → destination."""
    # 0) Resolve endpoints
    o = resolve_point(origin, ors=ors)
    d = resolve_point(destination, ors=ors)

    # 1) Load ports and choose nearest to each endpoint (by gate)
    ports = ports_index.load_ports()
    p_o, _ = find_nearest_port(o["lat"], o["lon"], ports, use_gate=True)
    p_d, _ = find_nearest_port(d["lat"], d["lon"], ports, use_gate=True)

    # 2) Effective road points to route to/from
    rp_o = _roadpoint_for_port(p_o)
    rp_d = _roadpoint_for_port(p_d)

    # 3) Assumptions
    A = {**DEFAULTS, **overrides}

    # 4) Legs
    leg1 = _road_leg(
          o
        , rp_o
        , ors=ors
        , rate_brl_km=A["ROAD_COST_PER_KM"]
        , co2_t_km=A["ROAD_CO2_T_PER_KM"]
    )
    sea  = _sea_leg(
          {"name": p_o["name"], "lat": p_o["lat"], "lon": p_o["lon"]}
        , {"name": p_d["name"], "lat": p_d["lat"], "lon": p_d["lon"]}
        , sea_speed_kmh=A["SEA_SPEED_KMH"]
        , sea_cost_km=A["SEA_COST_PER_KM"]
        , sea_co2_km=A["SEA_CO2_T_PER_KM"]
    )
    # add handling at both ports as dwell (hours) + fixed cost
    sea["hours"] += 2 * float(A["PORT_HANDLING_HOURS"])
    sea["cost_brl"] += 2 * float(A["PORT_HANDLING_COST"])

    leg3 = _road_leg(
          rp_d
        , d
        , ors=ors
        , rate_brl_km=A["ROAD_COST_PER_KM"]
        , co2_t_km=A["ROAD_CO2_T_PER_KM"]
    )

    legs = [leg1, sea, leg3]
    totals = {
          "distance_km": sum(l["distance_km"] for l in legs)
        , "hours":       sum(l["hours"]       for l in legs)
        , "cost_brl":    sum(l["cost_brl"]    for l in legs)
        , "co2eq_t":     sum(l["co2eq_t"]     for l in legs)
    }

    return {
          "origin": o
        , "destination": d
        , "ports_used": [{"name": p_o["name"], "lat": p_o["lat"], "lon": p_o["lon"]},
                         {"name": p_d["name"], "lat": p_d["lat"], "lon": p_d["lon"]}]
        , "legs": legs
        , "totals": totals
        , "assumptions": A
    }
