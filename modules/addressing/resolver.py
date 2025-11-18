# modules/addressing/resolver.py
# -*- coding: utf-8 -*-
"""
High-level address / CEP / coordinate resolver (with UF support).

Produces GeoPoint objects from:
- (lat,lon)
- dicts (lat/lon, structured)
- CEPs
- free text

The goal is to centralise all logic that knows how to go from an arbitrary
"user location" input to a **GeoPoint(lat, lon, uf, label)**, using the
existing addressing helpers (coords, cep) and ORS geocoding.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from modules.core.models import GeoPoint
from modules.core.config import get_project_config
from modules.core.types import CoordinatePair, HasLatLon  # kept for type hints elsewhere
from modules.infra.logging import get_logger

from modules.addressing.coords import (
      parse_latlon_str
    , filter_hits
)
from modules.addressing.cep import (
      parse_cep
    , resolve_cep
    , viacep_lookup
)

_log = get_logger(__name__)


# ------------------------------------------------------------------------------
# Small helpers
# ------------------------------------------------------------------------------

def _make_point(
      lat: float
    , lon: float
    , uf: Optional[str]
    , label: str
) -> GeoPoint:
    """
    Centralised GeoPoint ctor so we always normalise UF/label the same way.
    """
    uf_clean = (uf or "").strip().upper()
    return GeoPoint(
          lat=float(lat)
        , lon=float(lon)
        , uf=uf_clean
        , label=str(label)
    )


def _hit_to_point(
      h: Dict[str, Any]
    , uf: Optional[str] = None
) -> GeoPoint:
    """
    Convert a normalised hit from coords.filter_hits into a GeoPoint.

    `h` is expected to have at least 'lat', 'lon' and optionally 'label'.
    If the hit already carries a 'uf' key, that wins over the explicit `uf`
    argument (so that future versions of coords.normalize_hit can enrich hits
    without having to change this resolver again).
    """
    lat = float(h["lat"])
    lon = float(h["lon"])
    label = h.get("label") or f"{lat:.6f},{lon:.6f}"
    uf_local = h.get("uf") if isinstance(h, dict) else None
    return _make_point(
          lat=lat
        , lon=lon
        , uf=(uf_local or uf)
        , label=label
    )


def _infer_uf_from_features(features: Any) -> Optional[str]:
    """
    Try to infer the Brazilian UF code from raw Pelias/ORS features.

    This only looks at the feature *properties* and never parses human
    labels, so we stay aligned with the geocoding provider instead of
    inventing our own heuristics.

    We purposely keep this conservative:
      - we only accept 2-letter alphabetic codes (e.g. 'SP', 'CE').
      - anything else is ignored (resolver will return UF="").
    """
    # geocode_structured returns {"features":[...]} while geocode_text
    # already returns the list. Normalise both shapes here.
    if isinstance(features, dict):
        features = (features or {}).get("features") or []

    if not isinstance(features, list):
        return None

    for feat in features:
        if not isinstance(feat, dict):
            continue
        props = feat.get("properties") or {}
        uf = (
              props.get("region_a")
            or props.get("region")
            or props.get("state")
        )
        if not uf:
            continue
        uf = str(uf).strip().upper()
        # Only trust proper 2-letter codes; otherwise let caller fall back.
        if len(uf) == 2 and uf.isalpha():
            return uf

    return None


def _uf_from_cep(value: str, *, ors) -> Optional[str]:
    """
    Best-effort UF lookup for a CEP using ViaCEP via cep.viacep_lookup.

    We keep this separate from resolve_cep because the resolver needs UF
    even when resolve_cep ends up using pure ORS geocoding.
    """
    digits = parse_cep(value)
    if not digits:
        return None

    if not getattr(ors.cfg, "allow_viacep", True):
        return None

    try:
        via = getattr(ors, "_viacep_lookup", viacep_lookup)(digits)
    except Exception as exc:  # pragma: no cover - network issues
        _log.warning("ViaCEP lookup failed for CEP=%s (%s)", digits, exc)
        return None

    if not via:
        return None

    uf = (via.get("uf") or "").strip().upper()
    return uf or None


# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------

def resolve_point(value: Any, *, ors) -> GeoPoint:
    """
    Returns a GeoPoint for any accepted location format.

    It relies on:
      - modules.addressing.coords   → for parsing and Pelias hit filtering
      - modules.addressing.cep      → for CEP recognition + ViaCEP
      - ORS geocoding (through `ors`)
    """

    default_country = getattr(
          ors.cfg
        , "default_country"
        , get_project_config().default_country
    )

    # -------------------------------------------------------------
    # Case 1 — tuple/list (lat, lon)
    # -------------------------------------------------------------
    if isinstance(value, (list, tuple)) and len(value) == 2:
        lat, lon = float(value[0]), float(value[1])
        # With bare coordinates we have no UF information; leave it empty.
        return _make_point(
              lat=lat
            , lon=lon
            , uf=None
            , label=f"{lat:.6f},{lon:.6f}"
        )

    # -------------------------------------------------------------
    # Case 2 — dict with explicit lat/lon
    # -------------------------------------------------------------
    if isinstance(value, dict) and {"lat", "lon"}.issubset(value):
        lat = float(value["lat"])
        lon = float(value["lon"])
        label = value.get("label", f"{lat:.6f},{lon:.6f}")
        uf = value.get("uf")  # allow callers to pre-populate UF explicitly
        return _make_point(
              lat=lat
            , lon=lon
            , uf=uf
            , label=label
        )

    # -------------------------------------------------------------
    # Case 3 — dict with structured fields
    # -------------------------------------------------------------
    if isinstance(value, dict):
        street      = value.get("street")
        housenumber = value.get("housenumber")
        locality    = value.get("locality")
        region      = value.get("region")
        postalcode  = value.get("postalcode")
        country     = value.get("country") or default_country

        # CEP-only dict
        if postalcode and not any([street, housenumber, locality, region]):
            uf = _uf_from_cep(str(postalcode), ors=ors)
            r = resolve_cep(str(postalcode), ors=ors)
            return _hit_to_point(r, uf=uf)

        # full structured dict
        if any([street, housenumber, locality, region, postalcode]):
            raw = ors.geocode_structured(
                  street=street
                , housenumber=housenumber
                , locality=locality
                , region=region
                , postalcode=postalcode
                , country=country
                , size=1
            )
            feats = (raw or {}).get("features") or []
            hits = filter_hits(raw)
            uf = _infer_uf_from_features(feats)

            if hits:
                return _hit_to_point(hits[0], uf=uf)

            # fallback text variants
            variants: List[str] = []

            if housenumber and street and locality and region:
                variants.append(f"{housenumber} {street}, {locality}, {region}, {country}")

            if street and locality and region:
                variants.append(f"{street}, {locality}, {region}, {country}")

            if street and locality:
                variants.append(f"{street}, {locality}, {country}")

            if locality and region:
                variants.append(f"{locality}, {region}, {country}")

            if locality:
                variants.append(f"{locality}, {country}")

            if postalcode:
                digits = parse_cep(str(postalcode))
                if digits:
                    hyph = f"{digits[:5]}-{digits[5:]}"
                    variants.append(hyph)

            tried = set()
            for q in variants:
                if not q or q in tried:
                    continue
                tried.add(q)

                raw2 = ors.geocode_text(q, size=1, country=country)
                feats2 = raw2  # geocode_text already returns a list[feature]
                hits2 = filter_hits(raw2)
                uf2 = _infer_uf_from_features(feats2)

                if hits2:
                    return _hit_to_point(hits2[0], uf=uf2)

            raise ValueError(f"Structured geocode failed for: {value}")

    # -------------------------------------------------------------
    # Case 4 — string
    # -------------------------------------------------------------
    if isinstance(value, str):
        s = value.strip()

        # "lat,lon"
        latlon = parse_latlon_str(s)
        if latlon:
            lat, lon = latlon
            # No clean UF from bare coordinates; leave blank.
            return _make_point(
                  lat=lat
                , lon=lon
                , uf=None
                , label=f"{lat:.6f},{lon:.6f}"
            )

        # CEP
        if parse_cep(s):
            uf = _uf_from_cep(s, ors=ors)
            r = resolve_cep(s, ors=ors)
            return _hit_to_point(r, uf=uf)

        # free text → ORS text
        raw = ors.geocode_text(s, size=1, country=default_country)
        feats = raw  # list[feature]
        hits = filter_hits(raw)
        uf = _infer_uf_from_features(feats)
        if hits:
            return _hit_to_point(hits[0], uf=uf)

        raise ValueError(f"Could not geocode text: '{value}'")

    # -------------------------------------------------------------
    # Unsupported
    # -------------------------------------------------------------
    raise TypeError(
        f"Unsupported point type: {type(value).__name__}. "
        f"Use str, dict, tuple, list."
    )


def resolve_point_null_safe(value: Any, *, ors, log=None) -> Optional[GeoPoint]:
    try:
        return resolve_point(value, ors=ors)
    except Exception as exc:
        (log or _log).warning(
            "Null-safe geocode failed for: %r → NULL (%s: %s)",
            value,
            type(exc).__name__,
            exc,
        )
        return None


if __name__ == "__main__":
    """
    Quick manual test:

    Examples:
        python -m modules.addressing.resolver "avenida luciano gualberto, 380"
        python -m modules.addressing.resolver "01310-200"
        python -m modules.addressing.resolver "-23.555673,-46.730133"
    """
    import json
    import sys

    from modules.road.ors_common import ORSConfig
    from modules.road.ors_client import ORSClient

    # Optional logging bootstrap
    try:
        from modules.infra.logging import init_logging  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive
        init_logging = None

    if init_logging is not None:
        init_logging(level="INFO", force=True, write_output=True)

    ors = ORSClient(cfg=ORSConfig())

    # If no args: use a small demo set
    args = sys.argv[1:]
    if not args:
        args = [
            "avenida luciano gualberto, 380",
            "01310-200",
            "-23.555673,-46.730133",
        ]

    print(">>> Resolver smoke test")
    for raw in args:
        try:
            result = resolve_point(raw, ors=ors)

            lat = float(getattr(result, "lat"))
            lon = float(getattr(result, "lon"))
            label = getattr(result, "label", f"{lat:.6f},{lon:.6f}")
            uf = getattr(result, "uf", "")

            data = {
                  "lat": lat
                , "lon": lon
                , "uf": uf
                , "label": label
            }

            print(f"\nINPUT : {raw!r}")
            print("OUTPUT:")
            print(json.dumps(data, ensure_ascii=False, indent=2))

        except Exception as exc:
            print(f"\nINPUT : {raw!r}")
            print(f"ERROR : {type(exc).__name__}: {exc}")
