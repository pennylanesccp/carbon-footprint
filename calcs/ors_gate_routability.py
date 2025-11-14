#!/usr/bin/env python3
# scripts/ors_gate_routability.py
# -*- coding: utf-8 -*-

from __future__ import annotations

# --- repo path bootstrap ---
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ---------------------------

import argparse
import json
from typing import Dict, Any, List

from modules.functions._logging import init_logging, get_logger
from modules.app.evaluator import DataPaths
from modules.road.addressing import resolve_point
from modules.road.ors_common import ORSConfig, NoRoute
from modules.road.ors_client import ORSClient

log = get_logger(__name__)

def _load_ports(path: Path) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _pick_gate(p: Dict[str, Any]) -> Dict[str, float]:
    gates = p.get("gates") or []
    if isinstance(gates, list) and gates:
        g = gates[0]
        return {"label": g.get("label","gate"), "lat": float(g["lat"]), "lon": float(g["lon"])}
    return {"label": p.get("name","port"), "lat": float(p["lat"]), "lon": float(p["lon"])}

def _route_km(ors: ORSClient, src: Dict[str,float], dst: Dict[str,float], profile: str) -> float:
    data = ors.route_road(
        {"lat": src["lat"], "lon": src["lon"], "label": src.get("label","src")},
        {"lat": dst["lat"], "lon": dst["lon"], "label": dst.get("label","dst")},
        profile=profile
    )
    return float(data["distance_m"]) / 1000.0

def main(argv=None) -> int:
    ap = argparse.ArgumentParser("Check ORS routability from port gates to their city centers.")
    ap.add_argument("--ports-json", type=Path, default=DataPaths().ports_json)
    ap.add_argument("--profile", default="driving-hgv", choices=["driving-hgv","driving-car"])
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    ap.add_argument("--pretty", action="store_true")
    args = ap.parse_args(argv)

    init_logging(level=args.log_level, force=True, write_output=False)

    ports = _load_ports(args.ports_json)
    cfg = ORSConfig()  # ← no 'profile' kwarg here
    ors = ORSClient(cfg=cfg)
    log.info("ORS client ready (primary profile=%s).", args.profile)

    results = []
    for p in ports:
        name = p.get("name")
        city = p.get("city")
        uf   = p.get("state")
        gate = _pick_gate(p)

        # Destination = "<city>, <UF>" center
        dst = resolve_point(f"{city}, {uf}", ors=ors)

        rec: Dict[str, Any] = {
            "port": name,
            "uf": uf,
            "gate_label": gate["label"],
            "gate_lat": gate["lat"],
            "gate_lon": gate["lon"],
            "dest_label": dst.get("label"),
            "dest_lat": float(dst["lat"]),
            "dest_lon": float(dst["lon"]),
            "hgv_ok_km": None,
            "car_ok_km": None,
            "error": None
        }

        primary = args.profile
        try:
            km_primary = _route_km(ors, gate, dst, primary)
            if primary == "driving-hgv":
                rec["hgv_ok_km"] = round(km_primary, 1)
            else:
                rec["car_ok_km"] = round(km_primary, 1)
        except NoRoute as e:
            rec["error"] = f"{primary} no-route: {e}"
            # Try the other profile as diagnostic
            alt = "driving-car" if primary == "driving-hgv" else "driving-hgv"
            try:
                km_alt = _route_km(ors, gate, dst, alt)
                key = "car_ok_km" if alt == "driving-car" else "hgv_ok_km"
                rec[key] = round(km_alt, 1)
            except Exception as e2:
                rec["error"] += f" | {alt} no-route: {e2}"
        except Exception as e:
            rec["error"] = f"Unexpected: {e}"

        results.append(rec)
        status = ("OK(HGV)" if rec["hgv_ok_km"] is not None
                  else "OK(CAR)" if rec["car_ok_km"] is not None
                  else "FAIL")
        log.info("%-30s | gate=%-18s | %s | km_hgv=%s km_car=%s",
                 name[:30], (gate['label'] or '')[:18], status, rec["hgv_ok_km"], rec["car_ok_km"])

    if args.pretty:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(results, ensure_ascii=False, separators=(",", ":")))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
