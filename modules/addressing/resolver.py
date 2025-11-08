# modules/addressing/resolver.py
# Address/CEP/coords resolver, with ViaCEP fallback and robust structured fallbacks.
# Works with an ORS-like client injected via `ors` (duck-typed: must expose
#   geocode_text(text, size, country) and geocode_structured(...)).

from __future__ import annotations

import re as _re
import json as _json
from typing import Any as _Any, Dict as _Dict, List as _List, Optional as _Optional
import logging as _logging
import requests as _req

_log = _logging.getLogger("cabosupernet.addressing.resolver")


# ────────────────────────────────────────────────────────────────────────────────
# Generic helpers
# ────────────────────────────────────────────────────────────────────────────────
def _is_latlon_str(text: str) -> _Optional[tuple[float, float]]:
    if not isinstance(text, str):
        return None
    s = text.strip()
    m = _re.match(r"^\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*$", s)
    if not m:
        return None
    lat = float(m.group(1))
    lon = float(m.group(2))
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return (lat, lon)


def _is_cep(text: str) -> _Optional[str]:
    if not isinstance(text, str):
        return None
    m = _re.match(r"^\s*(\d{5})-?(\d{3})\s*$", text)
    if not m:
        return None
    return f"{m.group(1)}{m.group(2)}"  # digits only


def _reject_centroid(lat: float, lon: float) -> bool:
    # Avoid Brazil centroid (common when Pelias returns country-level feature)
    return abs(lat - (-10.0)) < 0.5 and abs(lon - (-55.0)) < 0.5


def _normalize_hit_dict(f: _Dict) -> _Optional[_Dict]:
    """
    Accepts multiple shapes and normalizes to:
      {lat: float, lon: float, label: str|None, layer: str|None}
    Supported shapes:
      • {lat, lon, label?, layer?}
      • Pelias Feature-like: {geometry:{coordinates:[lon,lat]}, properties:{label/name/layer}}
    """
    lat = lon = None
    layer = (f.get("layer") or "").lower()
    label = f.get("label") or f.get("name")

    if "lat" in f and "lon" in f:
        try:
            lat = float(f["lat"])
            lon = float(f["lon"])
        except Exception:
            return None
    else:
        geom = f.get("geometry") or {}
        coords = (geom.get("coordinates") or [])
        props = f.get("properties") or {}
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


def _filter_hits(hits: _Any, allowed_layers: _Optional[_List[str]] = None) -> _List[_Dict]:
    """
    Defensive, tolerant filter:
      • Accepts list/dict/string/None
      • If dict has 'features', uses that list
      • Skips items that aren't dicts (tries json.loads for strings)
      • Rejects country-level/centroid results
      • Enforces layer allowlist if provided
    """
    allowed = allowed_layers or [
        "address", "street", "venue", "postalcode", "postcode",
        "neighbourhood", "locality", "localadmin", "borough", "municipality"
    ]

    # Normalize the container
    if hits is None:
        arr = []
    elif isinstance(hits, list):
        arr = hits
    elif isinstance(hits, dict):
        arr = hits.get("features") or [hits]
    else:
        arr = [hits]

    out: _List[_Dict] = []
    for item in arr:
        # Decode string payloads if any stray through
        if isinstance(item, str):
            try:
                item = _json.loads(item)
            except Exception:
                continue
        if not isinstance(item, dict):
            continue

        norm = _normalize_hit_dict(item)
        if not norm:
            # maybe wrapped as Pelias Feature in 'features' (already handled), else skip
            continue

        lat = norm["lat"]; lon = norm["lon"]
        layer = (norm.get("layer") or "").lower()
        label = norm.get("label")

        if layer == "country":
            continue
        if _reject_centroid(lat, lon):
            continue
        if allowed and layer and layer not in allowed:
            continue

        out.append({"lat": lat, "lon": lon, "label": label, "layer": layer})
    return out


def _viacep_lookup(cep: str) -> _Optional[_Dict[str, str]]:
    url = f"https://viacep.com.br/ws/{cep}/json/"
    _log.info(f"ViaCEP GET {url}")
    try:
        r = _req.get(url, timeout=(4.0, 10.0), headers={"User-Agent": "Cabosupernet-Resolver/1.0"})
        if not r.ok:
            _log.warning(f"ViaCEP status={r.status_code} text={(r.text or '')[:120]}")
            return None
        data = r.json()
        if data.get("erro"):
            return None
        return {
              "logradouro": data.get("logradouro") or ""
            , "bairro":     data.get("bairro") or ""
            , "localidade": data.get("localidade") or ""
            , "uf":         data.get("uf") or ""
        }
    except Exception as e:
        _log.warning(f"ViaCEP exception {type(e).__name__}: {e}")
        return None


# ────────────────────────────────────────────────────────────────────────────────
# CEP resolution with robust fallbacks (ORS structured → ORS text → ViaCEP)
# ────────────────────────────────────────────────────────────────────────────────
def _resolve_cep(value: str, *, ors) -> dict:
    """
    Resolve a Brazilian CEP (with or without hyphen) into lat/lon and label.

    Strategy:
      1) ORS structured search with numeric CEP (01310200)
      2) ORS structured search with hyphen     (01310-200)
      3) ORS text search with hyphen           (“01310-200”)
      4) ViaCEP → build “logradouro, bairro, localidade, UF” → ORS text
    """
    assert isinstance(value, str)
    cep_digits = _is_cep(value)
    if not cep_digits:
        raise ValueError(f"Not a valid CEP: {value}")
    cep_hyph = f"{cep_digits[:5]}-{cep_digits[5:]}"
    country = getattr(ors.cfg, "default_country", "BR")

    # 1) ORS structured (numeric)
    raw = ors.geocode_structured(postalcode=cep_digits, country=country, size=1)
    hits = _filter_hits(raw, allowed_layers=["postalcode", "postcode", "address", "street", "locality", "neighbourhood"])
    if hits:
        h = hits[0]
        out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or cep_hyph}
        _log.info(f"RESOLVE (CEP via ORS-structured) -> {out}")
        return out

    # 2) ORS structured (hyphenated)
    raw = ors.geocode_structured(postalcode=cep_hyph, country=country, size=1)
    hits = _filter_hits(raw, allowed_layers=["postalcode", "postcode", "address", "street", "locality", "neighbourhood"])
    if hits:
        h = hits[0]
        out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or cep_hyph}
        _log.info(f"RESOLVE (CEP via ORS-structured hyphen) -> {out}")
        return out

    # 3) ORS text (hyphen) — restrict to postal layers only
    raw = ors.geocode_text(cep_hyph, size=1, country=country)
    hits = _filter_hits(raw, allowed_layers=["postalcode", "postcode"])
    if hits:
        h = hits[0]
        out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or cep_hyph}
        _log.info(f"RESOLVE (CEP via ORS-text) -> {out}")
        return out

    # 4) ViaCEP fallback (if allowed)
    if getattr(ors.cfg, "allow_viacep", True):
        via = getattr(ors, "_viacep_lookup", _viacep_lookup)(cep_digits)
        if via and (via.get("localidade") or via.get("uf") or via.get("logradouro")):
            query = ", ".join([x for x in [via.get("logradouro",""), via.get("bairro",""), via.get("localidade",""), via.get("uf","")] if x])
            _log.info(f"RESOLVE (CEP ViaCEP->text) query='{query}'")
            raw = ors.geocode_text(query, size=1, country=country)
            hits = _filter_hits(raw)
            if hits:
                h = hits[0]
                out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or query}
                _log.info(f"RESOLVE (CEP via ViaCEP) -> {out}")
                return out

    raise ValueError(f"CEP geocoding yielded no acceptable results for: {value}")


# ────────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────────
def resolve_point(value, *, ors) -> dict:
    """
    Normalize a user-provided 'point' into:
        { "lat": float, "lon": float, "label": str }

    Accepts:
      • (lat, lon) tuples/lists
      • dicts with {"lat","lon"} OR structured fields
        (street, housenumber, locality, region, postalcode, country)
      • strings:
          - "lat,lon"
          - Brazilian CEP (with/without hyphen)
          - free text (address/city/POI)

    Strategy (high-level):
      • (tuple/list) → pass-through
      • dict(lat/lon) → pass-through
      • dict(structured) → ORS structured → ORS text fallbacks
      • CEP → _resolve_cep(...) (ORS + ViaCEP fallback)
      • free text → ORS text
      • filtering → _filter_hits rejects country-centroid/etc.
    """
    country = getattr(ors.cfg, "default_country", "BR")

    # (lat,lon) tuple/list
    if isinstance(value, (list, tuple)) and len(value) == 2:
        lat, lon = float(value[0]), float(value[1])
        out = {"lat": lat, "lon": lon, "label": f"{lat:.6f},{lon:.6f}"}
        _log.info(f"RESOLVE (tuple/list) -> {out}")
        return out

    # dict with explicit lat/lon
    if isinstance(value, dict) and {"lat", "lon"}.issubset(value.keys()):
        lat = float(value["lat"]); lon = float(value["lon"])
        out = {"lat": lat, "lon": lon, "label": value.get("label", f"{lat:.6f},{lon:.6f}")}
        _log.info(f"RESOLVE (dict lat/lon) -> {out}")
        return out

    # dict with structured fields
    if isinstance(value, dict):
        street      = value.get("street")
        housenumber = value.get("housenumber")
        locality    = value.get("locality")
        region      = value.get("region")
        postalcode  = value.get("postalcode")
        ctry        = value.get("country") or country

        # Only CEP inside dict? treat as CEP
        if postalcode and not any([street, housenumber, locality, region]):
            return _resolve_cep(str(postalcode), ors=ors)

        if any([street, housenumber, locality, region, postalcode]):
            # 1) ORS structured
            raw = ors.geocode_structured(
                  street      = street
                , housenumber = housenumber
                , locality    = locality
                , region      = region
                , postalcode  = postalcode
                , country     = ctry
                , size        = 1
            )
            hits = _filter_hits(raw)
            if hits:
                h = hits[0]
                out = {"lat": float(h["lat"]), "lon": float(h["lon"]), "label": h.get("label") or "structured"}
                _log.info(f"RESOLVE (structured dict) -> {out}")
                return out

            # 2) Progressive ORS text fallbacks
            variants: _List[str] = []

            if housenumber and street and locality and region:
                variants.append(f"{housenumber} {street}, {locality}, {region}, {ctry}")
            if housenumber and street and locality:
                variants.append(f"{housenumber} {street}, {locality}, {ctry}")

            if street and locality and region:
                variants.append(f"{street}, {locality}, {region}, {ctry}")
            if street and locality:
                variants.append(f"{street}, {locality}, {ctry}")

            if locality and region:
                variants.append(f"{locality}, {region}, {ctry}")
            if locality:
                variants.append(f"{locality}, {ctry}")

            if postalcode:
                _digits = _is_cep(str(postalcode))
                if _digits:
                    _hyph = f"{_digits[:5]}-{_digits[5:]}"
                    if housenumber and street and locality and region:
                        variants.insert(0, f"{housenumber} {street}, {locality}, {region}, {_hyph}, {ctry}")
                    variants.append(_hyph)

            tried = set()
            for q in [v for v in variants if v and v not in tried]:
                tried.add(q)
                _log.info(f"GEOCODE fallback text='{q}' country={ctry} size=1")
                raw2 = ors.geocode_text(q, size=1, country=ctry)
                hits2 = _filter_hits(raw2)
                if hits2:
                    h = hits2[0]
                    out = {"lat": float(h["lat"]), "lon": float(h["lon"]), "label": h.get("label") or q}
                    _log.info(f"RESOLVE (structured→text fallback) -> {out}")
                    return out

            raise ValueError(f"Structured geocoding yielded no acceptable results for: {value}")

    # string inputs
    if isinstance(value, str):
        s = value.strip()

        maybe = _is_latlon_str(s)
        if maybe:
            lat, lon = maybe
            out = {"lat": lat, "lon": lon, "label": f"{lat:.6f},{lon:.6f}"}
            _log.info(f"RESOLVE (lat,lon string) -> {out}")
            return out

        cep_digits = _is_cep(s)
        if cep_digits:
            return _resolve_cep(s, ors=ors)

        raw = ors.geocode_text(s, size=1, country=country)
        hits = _filter_hits(raw)
        if hits:
            h = hits[0]
            out = {"lat": float(h["lat"]), "lon": float(h["lon"]), "label": h.get("label") or s}
            _log.info(f"RESOLVE (text) -> {out}")
            return out

        raise ValueError(f"Geocoding yielded no acceptable results for: {value}")

    # unsupported type
    raise TypeError(
        "Unsupported point type. Use string (address/CEP/city/'lat,lon'), "
        "dicts (lat/lon or structured), or (lat,lon) tuple/list."
    )
