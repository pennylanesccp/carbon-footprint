# modules/cabotage/router.py
# High-level "door-to-door" cabotage finder: pick CT_o and CT_d by truck time,
# compute pre/on-carriage via ORS (driving-hgv), and stitch a simple sea leg
# placeholder (haversine) — you’ll replace with proforma-based CT↔CT costs later.

from __future__ import annotations
from typing import Any, Dict, List
import math
from modules.road.ors_client import ORSClient
from modules.addressing.resolver import resolve_point
from modules.cabotage.ports_index import nearest_ct_by_hgv_time

def _km(a, b) -> float:
    R = 6371.0
    lat1, lon1 = math.radians(a["lat"]), math.radians(a["lon"])
    lat2, lon2 = math.radians(b["lat"]), math.radians(b["lon"])
    dlat = lat2 - lat1; dlon = lon2 - lon1
    s = (math.sin(dlat/2)**2 +
         math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2)
    return 2*R*math.asin(math.sqrt(s))

def find_cabotage_route(
      origin: Any
    , destination: Any
    , *
    , ors: ORSClient
    , cargo_value_brl: float = 200_000.0
) -> Dict[str, Any]:
    # 1) resolve origin/destination
    o_pt = resolve_point(origin, ors=ors)
    d_pt = resolve_point(destination, ors=ors)

    # 2) nearest CTs by truck time
    o_ct = nearest_ct_by_hgv_time(o_pt, ors=ors)   # {ct, duration_s, distance_m, origin}
    d_ct = nearest_ct_by_hgv_time(d_pt, ors=ors)

    # 3) pre-carriage (city -> CT_o)
    pre = ors.route_road(o_pt, { "lat": o_ct["ct"]["lat"], "lon": o_ct["ct"]["lon"], "label": o_ct["ct"]["name"] })

    # 4) sea leg (CT_o -> CT_d) – placeholder by geodesic; replace with proforma/schedule later
    sea_km   = _km({ "lat": o_ct["ct"]["lat"], "lon": o_ct["ct"]["lon"] }
                 , { "lat": d_ct["ct"]["lat"], "lon": d_ct["ct"]["lon"] })
    sea_kn   = 16.0                      # nominal service speed
    sea_hrs  = (sea_km / 1.852) / sea_kn # km→nm then /kn = hours
    # crude placeholders for now:
    sea_cost_brl = 1500.0 + 0.85 * sea_km
    sea_co2_t    = 0.00006 * sea_km

    # 5) on-carriage (CT_d -> city)
    post = ors.route_road(
          { "lat": d_ct["ct"]["lat"], "lon": d_ct["ct"]["lon"], "label": d_ct["ct"]["name"] }
        , d_pt
    )

    legs: List[Dict[str, Any]] = [
          { "mode": "road", "from": o_pt["label"], "to": o_ct["ct"]["name"]
            , "distance_km": round((pre["distance_m"] or 0.0)/1000.0, 3)
            , "hours": round((pre["duration_s"] or 0.0)/3600.0, 3)
            , "cost_brl": None
            , "co2eq_t": None
          }
        , { "mode": "sea", "from": o_ct["ct"]["name"], "to": d_ct["ct"]["name"]
            , "distance_km": round(sea_km, 3)
            , "hours": round(sea_hrs, 3)
            , "cost_brl": round(sea_cost_brl, 2)
            , "co2eq_t": round(sea_co2_t, 4)
          }
        , { "mode": "road", "from": d_ct["ct"]["name"], "to": d_pt["label"]
            , "distance_km": round((post["distance_m"] or 0.0)/1000.0, 3)
            , "hours": round((post["duration_s"] or 0.0)/3600.0, 3)
            , "cost_brl": None
            , "co2eq_t": None
          }
    ]

    totals = {
          "distance_km": round(sum(x["distance_km"] for x in legs), 3)
        , "hours": round(sum(x["hours"] for x in legs), 3)
        , "cost_brl": round(sum(x["cost_brl"] or 0.0 for x in legs), 2)
        , "co2eq_t": round(sum(x["co2eq_t"] or 0.0 for x in legs), 4)
    }

    return {
          "origin_point": o_pt
        , "destination_point": d_pt
        , "ports_used": [o_ct["ct"]["code"], d_ct["ct"]["code"]]
        , "legs": legs
        , "totals": totals
    }
