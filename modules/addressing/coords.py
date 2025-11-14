# modules/addressing/coords.py
# -*- coding: utf-8 -*-

"""
Coordinate / geocoder-hit helpers for the addressing subsystem.

Uses:
- CoordinatePair and JSON types (from modules.core.types)
- GeoPoint (light conversion only in resolver)
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from modules.core.types import CoordinatePair, JSONValue
from modules.infra.logging import get_logger

_log = get_logger(__name__)


_ALLOWED_LAYERS_DEFAULT = [
    "address", "street", "venue", "postalcode", "postcode",
    "neighbourhood", "locality", "localadmin", "borough", "municipality"
]


# ------------------------------------------------------------------------------
# Parse "lat,lon"
# ------------------------------------------------------------------------------

def parse_latlon_str(text: str) -> Optional[CoordinatePair]:
    """
    Accepts 'lat,lon'. Returns (lat,lon) or None.
    """
    if not isinstance(text, str):
        return None

    m = re.match(r"^\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*$", text.strip())
    if not m:
        return None

    lat = float(m.group(1))
    lon = float(m.group(2))

    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None

    return lat, lon


# ------------------------------------------------------------------------------
# Reject centroid-like matches
# ------------------------------------------------------------------------------

def reject_brazil_centroid(lat: float, lon: float) -> bool:
    return abs(lat + 10.0) < 0.5 and abs(lon + 55.0) < 0.5


# ------------------------------------------------------------------------------
# Normalize raw ORS/Pelias hit
# ------------------------------------------------------------------------------

def normalize_hit(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize various ORS/Pelias hit shapes to a common dict:
        {"lat": float, "lon": float, "label": str|None, "layer": str|None}
    """
    lat = lon = None
    label = raw.get("label") or raw.get("name")
    layer = (raw.get("layer") or "").lower()

    # direct lat/lon
    if "lat" in raw and "lon" in raw:
        try:
            lat = float(raw["lat"])
            lon = float(raw["lon"])
        except Exception:
            return None

    else:
        # pelias/geojson
        geom = raw.get("geometry") or {}
        coords = geom.get("coordinates") or []
        props = raw.get("properties") or {}

        if len(coords) == 2:
            try:
                lon = float(coords[0])
                lat = float(coords[1])
            except Exception:
                return None

        label = label or props.get("label") or props.get("name")
        layer = layer or (props.get("layer") or "").lower()

    if lat is None or lon is None:
        return None

    return {"lat": lat, "lon": lon, "label": label, "layer": layer}


# ------------------------------------------------------------------------------
# Filter hits
# ------------------------------------------------------------------------------

def filter_hits(
    hits: Any,
    allowed_layers: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Returns a list of normalized:
        {"lat","lon","label","layer"}
    after rejecting:
    - invalid geojsons
    - "country" layer
    - Brazil centroid
    - non-allowed layers
    """
    allowed = allowed_layers or _ALLOWED_LAYERS_DEFAULT

    # normalize container
    if hits is None:
        arr = []
    elif isinstance(hits, list):
        arr = hits
    elif isinstance(hits, dict):
        arr = hits.get("features") or [hits]
    else:
        arr = [hits]

    out = []

    for item in arr:
        # decode stray JSON strings
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except Exception:
                continue

        if not isinstance(item, dict):
            continue

        norm = normalize_hit(item)
        if not norm:
            continue

        lat, lon = norm["lat"], norm["lon"]
        layer = (norm["layer"] or "").lower()

        if layer == "country":
            continue
        if reject_brazil_centroid(lat, lon):
            continue
        if allowed and layer and layer not in allowed:
            continue

        out.append(norm)

    return out
