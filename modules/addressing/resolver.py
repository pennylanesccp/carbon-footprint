# modules/addressing/resolver.py
# -*- coding: utf-8 -*-

"""
High-level address / CEP / coordinate resolver.

Produces GeoPoint objects from:
- (lat,lon)
- dicts (lat/lon, structured)
- CEPs
- free text
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from modules.core.models import GeoPoint
from modules.core.config import get_project_config
from modules.core.types import CoordinatePair, HasLatLon
from modules.infra.logging import get_logger

from modules.addressing.coords import (
    parse_latlon_str,
    filter_hits
)
from modules.addressing.cep import (
    parse_cep,
    resolve_cep
)

_log = get_logger(__name__)


# ------------------------------------------------------------------------------
# Convert dict hit → GeoPoint
# ------------------------------------------------------------------------------

def _hit_to_point(h: Dict[str, Any]) -> GeoPoint:
    return GeoPoint(lat=float(h["lat"]), lon=float(h["lon"]), label=h.get("label") or "")


# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------

def resolve_point(value: Any, *, ors) -> GeoPoint:
    """
    Returns a GeoPoint for any accepted location format.
    """

    default_country = getattr(
        ors.cfg, "default_country",
        get_project_config().default_country
    )

    # -------------------------------------------------------------
    # Case 1 — tuple/list (lat, lon)
    # -------------------------------------------------------------
    if isinstance(value, (list, tuple)) and len(value) == 2:
        lat, lon = float(value[0]), float(value[1])
        return GeoPoint(lat=lat, lon=lon, label=f"{lat:.6f},{lon:.6f}")

    # -------------------------------------------------------------
    # Case 2 — dict with explicit lat/lon
    # -------------------------------------------------------------
    if isinstance(value, dict) and {"lat", "lon"}.issubset(value):
        lat = float(value["lat"])
        lon = float(value["lon"])
        label = value.get("label", f"{lat:.6f},{lon:.6f}")
        return GeoPoint(lat=lat, lon=lon, label=label)

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
            r = resolve_cep(str(postalcode), ors=ors)
            return _hit_to_point(r)

        # full structured dict
        if any([street, housenumber, locality, region, postalcode]):
            raw = ors.geocode_structured(
                street=street,
                housenumber=housenumber,
                locality=locality,
                region=region,
                postalcode=postalcode,
                country=country,
                size=1
            )
            hits = filter_hits(raw)
            if hits:
                return _hit_to_point(hits[0])

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
                hits2 = filter_hits(raw2)
                if hits2:
                    return _hit_to_point(hits2[0])

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
            return GeoPoint(lat=lat, lon=lon, label=f"{lat:.6f},{lon:.6f}")

        # CEP
        if parse_cep(s):
            return _hit_to_point(resolve_cep(s, ors=ors))

        # free text → ORS text
        raw = ors.geocode_text(s, size=1, country=default_country)
        hits = filter_hits(raw)
        if hits:
            return _hit_to_point(hits[0])

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
        (log or _log).warning(f"Null-safe geocode failed for: {value} → NULL ({exc})")
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

            # Support both GeoPoint and dict outputs (old vs new versions)
            try:
                # GeoPoint-like
                lat = float(getattr(result, "lat"))
                lon = float(getattr(result, "lon"))
                label = getattr(result, "label", f"{lat:.6f},{lon:.6f}")
                data = {"lat": lat, "lon": lon, "label": label}
            except Exception:
                # dict-like (legacy behavior)
                if isinstance(result, dict):
                    data = {
                        "lat": float(result["lat"]),
                        "lon": float(result["lon"]),
                        "label": result.get("label"),
                    }
                else:
                    data = {"raw_result": repr(result)}

            print(f"\nINPUT : {raw!r}")
            print("OUTPUT:")
            print(json.dumps(data, ensure_ascii=False, indent=2))

        except Exception as exc:
            print(f"\nINPUT : {raw!r}")
            print(f"ERROR : {type(exc).__name__}: {exc}")
