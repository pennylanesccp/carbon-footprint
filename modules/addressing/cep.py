# modules/addressing/cep.py
# -*- coding: utf-8 -*-

"""
CEP recognition + resolution logic.
Integrates with:
- filter_hits from modules.addressing.coords
- GeoPoint from modules.core.models (returned by resolver.py)
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

import requests

from modules.infra.logging import get_logger
from modules.addressing.coords import filter_hits

_log = get_logger(__name__)


# ------------------------------------------------------------------------------
# CEP parsing
# ------------------------------------------------------------------------------

def parse_cep(text: str) -> Optional[str]:
    """
    Accepts: 01310-200 OR 01310200
    Returns: digits-only '01310200' or None
    """
    if not isinstance(text, str):
        return None

    m = re.match(r"^\s*(\d{5})-?(\d{3})\s*$", text)
    if not m:
        return None

    return f"{m.group(1)}{m.group(2)}"


# ------------------------------------------------------------------------------
# ViaCEP
# ------------------------------------------------------------------------------

def viacep_lookup(cep: str) -> Optional[Dict[str, str]]:
    """
    Raw ViaCEP call.
    """
    url = f"https://viacep.com.br/ws/{cep}/json/"

    try:
        r = requests.get(url, timeout=(4, 10))
        if not r.ok:
            return None
        data = r.json()
        if data.get("erro"):
            return None

        return {
            "logradouro": data.get("logradouro") or "",
            "bairro":     data.get("bairro") or "",
            "localidade": data.get("localidade") or "",
            "uf":         data.get("uf") or "",
        }

    except Exception:
        return None


# ------------------------------------------------------------------------------
# CEP resolution
# ------------------------------------------------------------------------------

def resolve_cep(value: str, *, ors: Any) -> Dict[str, Any]:
    """
    Returns dict:
       {"lat": float, "lon": float, "label": str}
    (Resolver converts to GeoPoint)
    """
    cep_digits = parse_cep(value)
    if not cep_digits:
        raise ValueError(f"Invalid CEP: {value}")

    country = getattr(ors.cfg, "default_country", "BR")
    hyph = f"{cep_digits[:5]}-{cep_digits[5:]}"

    # --- 1) ORS structured with digits
    raw = ors.geocode_structured(postalcode=cep_digits, country=country, size=1)
    hits = filter_hits(raw, allowed_layers=["postalcode", "postcode", "address", "street", "locality"])
    if hits:
        h = hits[0]
        return {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or hyph}

    # --- 2) ORS structured with hyphen
    raw = ors.geocode_structured(postalcode=hyph, country=country, size=1)
    hits = filter_hits(raw, allowed_layers=["postalcode", "postcode", "address", "street", "locality"])
    if hits:
        h = hits[0]
        return {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or hyph}

    # --- 3) text fallback
    raw = ors.geocode_text(hyph, size=1, country=country)
    hits = filter_hits(raw, allowed_layers=["postalcode", "postcode"])
    if hits:
        h = hits[0]
        return {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or hyph}

    # --- 4) ViaCEP → ORS text
    if getattr(ors.cfg, "allow_viacep", True):
        via = getattr(ors, "_viacep_lookup", viacep_lookup)(cep_digits)
        if via:
            query = ", ".join([via[k] for k in ("logradouro", "bairro", "localidade", "uf") if via[k]])
            if query:
                raw2 = ors.geocode_text(query, size=1, country=country)
                hits2 = filter_hits(raw2)
                if hits2:
                    h = hits2[0]
                    return {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or query}

    raise ValueError(f"CEP '{value}' could not be resolved")
