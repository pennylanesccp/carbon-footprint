# modules/cabotage/ports_index.py
# Gate-aware Brazilian ports loader (JSON) + backward-compat 'load_cts'

from __future__ import annotations
from typing import Any as _Any, Dict as _Dict, List as _List, Optional as _Opt, Tuple as _T
import json, os

_REQUIRED_KEYS = ("name", "city", "state", "lat", "lon")
_GATE_KEYS     = ("label", "lat", "lon")

def _as_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def _norm_gate(g: _Dict[str, _Any]) -> _Opt[_Dict[str, _Any]]:
    if not isinstance(g, dict):
        return None
    label = (g.get("label") or "").strip() or "gate"
    lat   = _as_float(g.get("lat"))
    lon   = _as_float(g.get("lon"))
    if lat is None or lon is None:
        return None
    return { "label": label, "lat": lat, "lon": lon }

def _norm_record(r: _Dict[str, _Any]) -> _Opt[_Dict[str, _Any]]:
    if not isinstance(r, dict):
        return None
    for k in _REQUIRED_KEYS:
        if k not in r:
            return None
    out = {
          "name":  str(r["name"]).strip()
        , "city":  str(r["city"]).strip()
        , "state": str(r["state"]).strip()
        , "lat":   _as_float(r["lat"])
        , "lon":   _as_float(r["lon"])
        , "aliases": []
        , "gates": []
    }
    if out["lat"] is None or out["lon"] is None:
        return None

    # aliases
    aliases = r.get("aliases") or []
    if isinstance(aliases, (list, tuple)):
        seen = set()
        for a in aliases:
            s = str(a).strip()
            if s and s.lower() not in seen:
                out["aliases"].append(s)
                seen.add(s.lower())

    # gates
    gates = r.get("gates") or []
    if isinstance(gates, (list, tuple)):
        gnorm = []
        for g in gates:
            g1 = _norm_gate(g)
            if g1:
                gnorm.append(g1)
        out["gates"] = gnorm

    return out

def load_ports(
      path: _Opt[str] = None
    , *
    , fallback: _Opt[_List[_Dict[str, _Any]]] = None
) -> _List[_Dict[str, _Any]]:
    """
    Load gate-aware ports from JSON (preferred). If 'path' is None, uses 'fallback' if provided,
    otherwise raises if nothing is available.
    """
    if path:
        if not os.path.exists(path):
            raise FileNotFoundError(f"ports JSON not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    else:
        if fallback is None:
            raise ValueError("No JSON path provided and no fallback data.")
        raw = fallback

    out: _List[_Dict[str, _Any]] = []
    for r in raw or []:
        rr = _norm_record(r)
        if rr:
            out.append(rr)
    if not out:
        raise ValueError("No valid port records after normalization.")
    return out

# Backward compatibility: some code/notebooks call this name
def load_cts(
      path: _Opt[str] = None
    , *
    , fallback: _Opt[_List[_Dict[str, _Any]]] = None
):
    return load_ports(path, fallback=fallback)
