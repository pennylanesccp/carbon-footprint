# modules/cabotage/ports_index.py
# -*- coding: utf-8 -*-
"""
Gate-aware Brazilian ports loader (JSON) + backward-compat 'load_cts'
=====================================================================

Purpose
-------
Load and normalize a list of Brazilian ports from JSON (or a provided fallback),
ensuring each record has the canonical fields and optional **gates** with coordinates.

Record shape (normalized)
-------------------------
{
  "name":    str,                 # canonical port name (e.g., "Santos (SP)")
  "city":    str,                 # city name
  "state":   str,                 # UF (2-letter)
  "lat":     float,               # port reference latitude
  "lon":     float,               # port reference longitude
  "aliases": List[str],           # optional aliases (deduped, trimmed)
  "gates":   List[{"label": str, "lat": float, "lon": float}]  # optional access gates
}

Public API (kept stable)
------------------------
- load_ports(path: Optional[str] = None, *, fallback: Optional[List[Dict[str,Any]]] = None)
    -> List[Dict[str, Any]]
- load_cts(...)  # alias for backward compatibility

Notes
-----
- If *path* is provided, it is used. If missing or None, *fallback* must be provided.
- Invalid records (missing required keys or bad coords) are ignored. If none survive,
  a ValueError is raised.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from modules.functions._logging import get_logger

_log = get_logger(__name__)

__all__ = ["load_ports", "load_cts"]

_REQUIRED_KEYS = ("name", "city", "state", "lat", "lon")
_GATE_KEYS = ("label", "lat", "lon")  # retained for documentation/reference


# ────────────────────────────────────────────────────────────────────────────────
# Helpers (normalization & coercion)
# ────────────────────────────────────────────────────────────────────────────────
def _as_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Best-effort float coercion with default fallback (does not raise)."""
    try:
        return float(x)
    except Exception:
        return default


def _norm_text(s: Any) -> str:
    """Trimmed string representation (None → '')."""
    return str(s or "").strip()


def _norm_gate(g: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a single gate entry.

    Accepts common shapes and returns None if lat/lon are invalid.
    """
    if not isinstance(g, dict):
        return None
    label = _norm_text(g.get("label")) or "gate"
    lat = _as_float(g.get("lat"))
    lon = _as_float(g.get("lon"))
    if lat is None or lon is None:
        return None
    return {"label": label, "lat": float(lat), "lon": float(lon)}


def _dedupe_aliases(aliases_in: Any, *, main_name: str) -> List[str]:
    """Case-insensitive, trimmed dedupe; drops empty and the same as main name."""
    aliases_out: List[str] = []
    if isinstance(aliases_in, (list, tuple)):
        seen = set()
        main_lower = _norm_text(main_name).lower()
        for a in aliases_in:
            s = _norm_text(a)
            if not s:
                continue
            k = s.lower()
            if k == main_lower:
                continue
            if k not in seen:
                aliases_out.append(s)
                seen.add(k)
    return aliases_out


def _norm_record(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a single port record.

    Returns None if required keys are missing or coordinates are invalid.
    """
    if not isinstance(r, dict):
        return None

    # Required keys present?
    for k in _REQUIRED_KEYS:
        if k not in r:
            return None

    name = _norm_text(r["name"])
    city = _norm_text(r["city"])
    state = _norm_text(r["state"])
    lat = _as_float(r["lat"])
    lon = _as_float(r["lon"])
    if lat is None or lon is None:
        return None

    # Aliases
    aliases = _dedupe_aliases(r.get("aliases") or [], main_name=name)

    # Gates (optional)
    gates_in = r.get("gates") or []
    gates: List[Dict[str, Any]] = []
    if isinstance(gates_in, (list, tuple)):
        for g in gates_in:
            g1 = _norm_gate(g)
            if g1:
                gates.append(g1)

    out: Dict[str, Any] = {
        "name": name,
        "city": city,
        "state": state,
        "lat": float(lat),
        "lon": float(lon),
        "aliases": aliases,
        "gates": gates,
    }
    return out


# ────────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────────
def load_ports(
    path: Optional[str] = None,
    *,
    fallback: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Load gate-aware ports from JSON file or fallback data.

    Parameters
    ----------
    path : Optional[str]
        JSON file path containing a list of port records.
    fallback : Optional[List[Dict[str, Any]]]
        In-memory list of raw port dicts used if *path* is None.

    Returns
    -------
    List[Dict[str, Any]]
        Normalized port records.

    Raises
    ------
    FileNotFoundError
        If *path* is provided but does not exist.
    ValueError
        If neither *path* nor *fallback* is provided, or if no valid records remain.
    """
    raw: List[Dict[str, Any]]

    if path:
        if not os.path.exists(path):
            raise FileNotFoundError(f"ports JSON not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            raw_loaded = json.load(f)
        if not isinstance(raw_loaded, list):
            raise ValueError(f"ports JSON at '{path}' must be a list of records.")
        raw = raw_loaded  # type: ignore[assignment]
        _log.info(f"load_ports: loaded {len(raw)} raw records from '{path}'.")
    else:
        if fallback is None:
            raise ValueError("No JSON path provided and no fallback data.")
        if not isinstance(fallback, list):
            raise ValueError("fallback must be a list of records.")
        raw = fallback
        _log.info(f"load_ports: using fallback with {len(raw)} raw records.")

    # Normalize
    out: List[Dict[str, Any]] = []
    invalid = 0
    for r in raw or []:
        rr = _norm_record(r)
        if rr:
            out.append(rr)
        else:
            invalid += 1

    if not out:
        _log.error("load_ports: no valid port records after normalization.")
        raise ValueError("No valid port records after normalization.")

    # Some quick stats
    with_gates = sum(1 for it in out if it.get("gates"))
    _log.info(
        "load_ports: normalized %d records (invalid skipped=%d, with_gates=%d).",
        len(out), invalid, with_gates
    )
    _log.debug(
        "load_ports: example[0]=%s",
        out[0] if out else None
    )

    return out


# Backward compatibility: some code/notebooks call this name
def load_cts(
    path: Optional[str] = None,
    *,
    fallback: Optional[List[Dict[str, Any]]] = None,
):
    """Alias to load_ports for backward compatibility."""
    return load_ports(path, fallback=fallback)


"""
Quick logging smoke test (PowerShell)
python -c `
"from modules.functions.logging import init_logging; `
from modules.cabotage.ports_index import load_ports, load_cts; `
init_logging(level='INFO', force=True, write_output=False); `
fallback = [ `
  { 'name':'Santos (SP)', 'city':'Santos', 'state':'SP', 'lat':-23.952, 'lon':-46.328, `
    'aliases':['Porto de Santos','santos (sp)'], `
    'gates':[{'label':'Ponta da Praia','lat':-23.986,'lon':-46.296}, {'lat':-23.97,'lon':-46.33}] }, `
  { 'name':'Rio de Janeiro (RJ)', 'city':'Rio de Janeiro', 'state':'RJ', 'lat':-22.903, 'lon':-43.172 } `
]; `
ports = load_ports(fallback=fallback); `
print('count=', len(ports)); `
print('first=', ports[0]); `
ports2 = load_cts(fallback=fallback); `
print('alias_count=', len(ports2)); "
"""
