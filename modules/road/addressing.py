# modules/addressing/resolver.py
# -*- coding: utf-8 -*-
"""
Address/CEP/coords resolver with robust fallbacks.

Duck-typed expectations for the injected `ors` client:
- ors.cfg.default_country  (str, default "BR")
- ors.cfg.allow_viacep     (bool, default True)
- ors.geocode_text(text: str, *, size: int, country: str) -> Any
- ors.geocode_structured(street=None, housenumber=None, locality=None,
                         region=None, postalcode=None, country=None, size=1) -> Any
Optional:
- ors._viacep_lookup(cep_digits: str) -> dict | None

Public API:
- resolve_point(value, *, ors) -> {"lat": float, "lon": float, "label": str}
    • Raises ValueError if no geocode is found.
- resolve_point_null_safe(value, ors=None, log=None) -> dict | None
    • Returns None instead of raising on "no geocode".
"""

from __future__ import annotations

import logging
import json
import re
from typing import Any, Dict, List, Optional, Tuple

import requests

from modules.functions._logging import get_logger

_log = get_logger(__name__)

# Default layer allowlist used when filtering ORS/Pelias hits
_ALLOWED_LAYERS_DEFAULT: List[str] = [
    "address", "street", "venue", "postalcode", "postcode",
    "neighbourhood", "locality", "localadmin", "borough", "municipality",
]


# ────────────────────────────────────────────────────────────────────────────────
# Small generic helpers
# ────────────────────────────────────────────────────────────────────────────────

def _is_latlon_str(text: str) -> Optional[Tuple[float, float]]:
    """
    Accepts 'lat,lon' in decimal degrees. Returns (lat,lon) or None.
    """
    if not isinstance(text, str):
        return None
    s = text.strip()
    m = re.match(r"^\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*$", s)
    if not m:
        return None
    lat = float(m.group(1))
    lon = float(m.group(2))
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return lat, lon


def _is_cep(text: str) -> Optional[str]:
    """
    Accepts '01310-200' or '01310200'. Returns digits-only '01310200' or None.
    """
    if not isinstance(text, str):
        return None
    m = re.match(r"^\s*(\d{5})-?(\d{3})\s*$", text)
    if not m:
        return None
    return f"{m.group(1)}{m.group(2)}"


def _reject_centroid(lat: float, lon: float) -> bool:
    """
    Rejects Brazil centroid-like results (frequent with country-only matches).
    """
    return abs(lat - (-10.0)) < 0.5 and abs(lon - (-55.0)) < 0.5


def _normalize_hit_dict(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize various geocoder shapes to:
        {"lat": float, "lon": float, "label": str|None, "layer": str|None}

    Supported input shapes:
    - {"lat":..., "lon":..., "label"?, "layer"?}
    - Pelias-like Feature: {"geometry":{"coordinates":[lon,lat]},
                            "properties":{"label"/"name"/"layer"}}
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
        coords = geom.get("coordinates") or []
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


def _filter_hits(hits: Any, allowed_layers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Defensive filter for geocoder hits:
    - accepts list/dict/string/None
    - if dict has 'features', use that list
    - skips non-dict items (tries json.loads for stray strings)
    - rejects country-level / Brazil centroid-only
    - enforces layer allowlist if provided

    Returns a list of normalized dicts: {"lat","lon","label","layer"}
    """
    allowed = allowed_layers or _ALLOWED_LAYERS_DEFAULT

    # Normalize container
    if hits is None:
        arr = []
    elif isinstance(hits, list):
        arr = hits
    elif isinstance(hits, dict):
        arr = hits.get("features") or [hits]
    else:
        arr = [hits]

    out: List[Dict[str, Any]] = []
    rejected_country = rejected_centroid = rejected_layer = 0

    for item in arr:
        # decode stray string payloads
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except Exception:
                continue
        if not isinstance(item, dict):
            continue

        norm = _normalize_hit_dict(item)
        if not norm:
            continue

        lat = norm["lat"]
        lon = norm["lon"]
        layer = (norm.get("layer") or "").lower()
        label = norm.get("label")

        if layer == "country":
            rejected_country += 1
            continue
        if _reject_centroid(lat, lon):
            rejected_centroid += 1
            continue
        if allowed and layer and layer not in allowed:
            rejected_layer += 1
            continue

        out.append({"lat": lat, "lon": lon, "label": label, "layer": layer})

    _log.debug(
        "FILTER hits: in=%s kept=%s rej_country=%s rej_centroid=%s rej_layer=%s",
        (len(arr) if isinstance(arr, list) else 1),
        len(out),
        rejected_country,
        rejected_centroid,
        rejected_layer,
    )
    return out


# ────────────────────────────────────────────────────────────────────────────────
# ViaCEP helper (used as fallback when CEP→ORS fails)
# ────────────────────────────────────────────────────────────────────────────────

def _viacep_lookup(cep: str) -> Optional[Dict[str, str]]:
    """
    Raw ViaCEP lookup for a CEP in digit form ('01310200').
    Safe to be replaced by 'ors._viacep_lookup' if provided.
    """
    url = f"https://viacep.com.br/ws/{cep}/json/"
    _log.info("ViaCEP GET %s", url)
    try:
        r = requests.get(
            url,
            timeout=(4.0, 10.0),
            headers={"User-Agent": "Cabosupernet-Resolver/1.0"},
        )
        if not r.ok:
            _log.warning("ViaCEP status=%s text=%s", r.status_code, (r.text or "")[:200])
            return None
        data = r.json()
        if data.get("erro"):
            _log.info("ViaCEP response indicates error for CEP=%s", cep)
            return None
        out = {
            "logradouro": data.get("logradouro") or "",
            "bairro":     data.get("bairro") or "",
            "localidade": data.get("localidade") or "",
            "uf":         data.get("uf") or "",
        }
        _log.debug("ViaCEP OK CEP=%s → %s", cep, out)
        return out
    except Exception as e:
        _log.warning("ViaCEP exception %s: %s", type(e).__name__, e)
        return None


# ────────────────────────────────────────────────────────────────────────────────
# CEP resolution (ORS structured → ORS text → ViaCEP)
# ────────────────────────────────────────────────────────────────────────────────

def _resolve_cep(value: str, *, ors) -> Dict[str, Any]:
    """
    Resolve a Brazilian CEP (with or without hyphen) into lat/lon and label.

    Strategy:
      1) ORS structured search with numeric CEP (01310200)
      2) ORS structured search with hyphen     (01310-200)
      3) ORS text search with hyphen           ("01310-200")
      4) ViaCEP → build "logradouro, bairro, localidade, UF" → ORS text
    """
    assert isinstance(value, str)
    cep_digits = _is_cep(value)
    if not cep_digits:
        raise ValueError(f"Not a valid CEP: {value}")
    cep_hyph = f"{cep_digits[:5]}-{cep_digits[5:]}"
    country = getattr(ors.cfg, "default_country", "BR")

    _log.debug("CEP resolve start: digits=%s hyph=%s country=%s", cep_digits, cep_hyph, country)

    # 1) ORS structured (numeric)
    raw = ors.geocode_structured(postalcode=cep_digits, country=country, size=1)
    hits = _filter_hits(
        raw,
        allowed_layers=["postalcode", "postcode", "address", "street", "locality", "neighbourhood"],
    )
    if hits:
        h = hits[0]
        out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or cep_hyph}
        _log.info("RESOLVE (CEP via ORS-structured) -> %s", out)
        return out

    # 2) ORS structured (hyphenated)
    raw = ors.geocode_structured(postalcode=cep_hyph, country=country, size=1)
    hits = _filter_hits(
        raw,
        allowed_layers=["postalcode", "postcode", "address", "street", "locality", "neighbourhood"],
    )
    if hits:
        h = hits[0]
        out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or cep_hyph}
        _log.info("RESOLVE (CEP via ORS-structured hyphen) -> %s", out)
        return out

    # 3) ORS text (hyphen) — restrict to postal layers only
    raw = ors.geocode_text(cep_hyph, size=1, country=country)
    hits = _filter_hits(raw, allowed_layers=["postalcode", "postcode"])
    if hits:
        h = hits[0]
        out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or cep_hyph}
        _log.info("RESOLVE (CEP via ORS-text) -> %s", out)
        return out

    # 4) ViaCEP fallback (if allowed)
    if getattr(ors.cfg, "allow_viacep", True):
        viacep_func = getattr(ors, "_viacep_lookup", _viacep_lookup)
        via = viacep_func(cep_digits)
        if via and (via.get("localidade") or via.get("uf") or via.get("logradouro")):
            query = ", ".join(
                [
                    x
                    for x in [
                        via.get("logradouro", ""),
                        via.get("bairro", ""),
                        via.get("localidade", ""),
                        via.get("uf", ""),
                    ]
                    if x
                ]
            )
            _log.info("RESOLVE (CEP ViaCEP->text) query='%s'", query)
            raw = ors.geocode_text(query, size=1, country=country)
            hits = _filter_hits(raw)
            if hits:
                h = hits[0]
                out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or query}
                _log.info("RESOLVE (CEP via ViaCEP) -> %s", out)
                return out

    _log.error("CEP geocoding failed: value=%s country=%s", value, country)
    raise ValueError(f"CEP geocoding yielded no acceptable results for: {value}")


# ────────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────────

def resolve_point(value: Any, *, ors) -> Dict[str, Any]:
    """
    Normalize a user-provided 'point' into a dict:
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
      • (tuple/list)           → pass-through
      • dict(lat/lon)          → pass-through
      • dict(structured)       → ORS structured → progressive ORS text fallbacks
      • CEP                    → _resolve_cep(...) (ORS + ViaCEP fallback)
      • free text              → ORS text
      • filtering              → _filter_hits rejects country-centroid/etc.

    On failure, raises ValueError.
    """
    country = getattr(ors.cfg, "default_country", "BR")
    _log.debug("resolve_point start: type=%s country=%s", type(value).__name__, country)

    # (lat,lon) tuple/list
    if isinstance(value, (list, tuple)) and len(value) == 2:
        lat, lon = float(value[0]), float(value[1])
        out = {"lat": lat, "lon": lon, "label": f"{lat:.6f},{lon:.6f}"}
        _log.info("RESOLVE (tuple/list) -> %s", out)
        return out

    # dict with explicit lat/lon
    if isinstance(value, dict) and {"lat", "lon"}.issubset(value.keys()):
        lat = float(value["lat"])
        lon = float(value["lon"])
        out = {
            "lat": lat,
            "lon": lon,
            "label": value.get("label", f"{lat:.6f},{lon:.6f}"),
        }
        _log.info("RESOLVE (dict lat/lon) -> %s", out)
        return out

    # dict with structured fields
    if isinstance(value, dict):
        street      = value.get("street")
        housenumber = value.get("housenumber")
        locality    = value.get("locality")
        region      = value.get("region")
        postalcode  = value.get("postalcode")
        ctry        = value.get("country") or country

        # CEP-only dict? Treat as CEP
        if postalcode and not any([street, housenumber, locality, region]):
            _log.debug("Structured dict contains only CEP, delegating to CEP resolver")
            return _resolve_cep(str(postalcode), ors=ors)

        if any([street, housenumber, locality, region, postalcode]):
            # 1) ORS structured
            _log.info(
                "GEOCODE structured street=%s housenumber=%s locality=%s region=%s "
                "postalcode=%s country=%s size=1",
                street,
                housenumber,
                locality,
                region,
                postalcode,
                ctry,
            )
            raw = ors.geocode_structured(
                street=street,
                housenumber=housenumber,
                locality=locality,
                region=region,
                postalcode=postalcode,
                country=ctry,
                size=1,
            )
            hits = _filter_hits(raw)
            if hits:
                h = hits[0]
                out = {
                    "lat": float(h["lat"]),
                    "lon": float(h["lon"]),
                    "label": h.get("label") or "structured",
                }
                _log.info("RESOLVE (structured dict) -> %s", out)
                return out

            # 2) Progressive ORS text fallbacks
            variants: List[str] = []

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
                digits = _is_cep(str(postalcode))
                if digits:
                    hyph = f"{digits[:5]}-{digits[5:]}"
                    if housenumber and street and locality and region:
                        variants.insert(
                            0,
                            f"{housenumber} {street}, {locality}, {region}, {hyph}, {ctry}",
                        )
                    variants.append(hyph)

            tried = set()
            for q in [v for v in variants if v and v not in tried]:
                tried.add(q)
                _log.info("GEOCODE fallback text='%s' country=%s size=1", q, ctry)
                raw2 = ors.geocode_text(q, size=1, country=ctry)
                hits2 = _filter_hits(raw2)
                if hits2:
                    h = hits2[0]
                    out = {
                        "lat": float(h["lat"]),
                        "lon": float(h["lon"]),
                        "label": h.get("label") or q,
                    }
                    _log.info("RESOLVE (structured→text fallback) -> %s", out)
                    return out

            _log.error("Structured geocoding failed for value=%s", value)
            raise ValueError(f"Structured geocoding yielded no acceptable results for: {value}")

    # string inputs
    if isinstance(value, str):
        s = value.strip()

        maybe = _is_latlon_str(s)
        if maybe:
            lat, lon = maybe
            out = {"lat": lat, "lon": lon, "label": f"{lat:.6f},{lon:.6f}"}
            _log.info("RESOLVE (lat,lon string) -> %s", out)
            return out

        cep_digits = _is_cep(s)
        if cep_digits:
            _log.debug("String looks like CEP, delegating to CEP resolver")
            return _resolve_cep(s, ors=ors)

        _log.info("GEOCODE text='%s' country=%s size=1", s, country)
        raw = ors.geocode_text(s, size=1, country=country)
        hits = _filter_hits(raw)
        if hits:
            h = hits[0]
            out = {"lat": float(h["lat"]), "lon": float(h["lon"]), "label": h.get("label") or s}
            _log.info("RESOLVE (text) -> %s", out)
            return out

        _log.error("Text geocoding failed for value='%s'", s)
        raise ValueError(f"Geocoding yielded no acceptable results for: {value}")

    # unsupported type
    _log.error("Unsupported point type: %s", type(value).__name__)
    raise TypeError(
        "Unsupported point type. Use string (address/CEP/city/'lat,lon'), "
        "dicts (lat/lon or structured), or (lat,lon) tuple/list."
    )


def resolve_point_null_safe(
    value: str,
    ors: Any | None = None,
    log: Optional[logging.Logger] = None,
) -> Optional[Dict[str, Any]]:
    """
    Wrapper around `resolve_point` that converts 'no geocode' into None
    instead of raising ValueError.

    Parameters
    ----------
    value : str
        Free-text address / city / CEP / coordinates.
    ors : Any, optional
        ORS client, if you pass it through.
    log : logging.Logger, optional
        Logger for warnings.

    Returns
    -------
    dict | None
        - dict with at least {'lat', 'lon', 'label'} if resolved
        - None if no acceptable geocode was found
    """
    try:
        return resolve_point(value=value, ors=ors)
    except ValueError as exc:
        msg = f"Geocoding yielded no acceptable results for: {value}"
        if log is not None:
            log.warning("%s — treating as NULL. Details: %s", msg, exc)
        else:
            _log.warning("%s — treating as NULL. Details: %s", msg, exc)
        return None


"""
────────────────────────────────────────────────────────────────────────────────
Quick logging smoke test (PowerShell)

python -c `
"from modules.functions.logging import init_logging; `
from modules.addressing.resolver import resolve_point; `
from modules.road.ors_common import ORSConfig; `
from modules.road.ors_client import ORSClient; import json; `
init_logging(level='INFO', force=True, write_output=True); `
ors = ORSClient(cfg=ORSConfig()); `
print('== TEXT =='); `
print(json.dumps(resolve_point('avenida luciano gualberto, 380', ors=ors), ensure_ascii=False, indent=2)); `
print(); `
print('== CEP =='); `
print(json.dumps(resolve_point('01310-200', ors=ors), ensure_ascii=False, indent=2)); `
print(); `
print('== STRUCT =='); `
print(json.dumps(resolve_point({'street':'Av. Paulista','housenumber':'1000','locality':'São Paulo','region':'SP'}, ors=ors), ensure_ascii=False, indent=2)); `
print(); `
print('== LATLON =='); `
print(json.dumps(resolve_point('-23.555673,-46.730133', ors=ors), ensure_ascii=False, indent=2))"

────────────────────────────────────────────────────────────────────────────────
"""
