#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build a heatmap-ready CSV of deltas between **cabotage** and **direct road**.

For each destination in a list:
  1) ROAD (direct O→D)
     a) ORS distance (primary profile = driving-hgv by default; can fallback to driving-car)
     b) Fuel/emissions/cost via modules.road.emissions.estimate_road_trip (single trip)

  2) CABOTAGE (door→door O→Po → Pd→D + sea leg + port ops + hotel)
     a) Nearest port to origin (by truck gate if available) → road fuel/emis/cost
     b) Port ops + hotel: fuel from handled mass (load & discharge) and per-city hotel factors
     c) Nearest port to destination
     d) Sea leg distance via SeaMatrix (fallback: haversine × coastline factor)
     e) Road Pd→D fuel/emis/cost

  3) Compute deltas: cabotage − road (negative = saving with cabotage)
     Output columns (minimal): destiny, delta_fuel_kg, delta_fuel_cost_brl, delta_co2e_kg
     Optional: add --with-geo to include destiny_lat, destiny_lon.

Requirements:
  • Repo layout with modules/... as imported below
  • Env var ORS_API_KEY set (picked up by ORSConfig)
  • Data files under modules/cabotage/_data/ (ports_br.json, sea_matrix.json, hotel.json)

Example:
  python build_heatmap_csv.py ^
    --origin "São Paulo, SP" ^
    --amount-tons 1000 ^
    --dest-file data/dests.txt ^
    --outdir outputs ^
    --truck semi_27t ^
    --diesel-price 6.20 ^
    --sea-K 0.0027 ^
    --mgo-price 3200 ^
    --ors-profile driving-hgv ^
    --fallback-to-car ^
    --with-geo
"""

from __future__ import annotations

import os
import csv
import argparse
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ────────────────────────────────────────────────────────────────────────────────
# Local package imports (expect this script to run at repo root)
# ────────────────────────────────────────────────────────────────────────────────
from modules.road.ors_common import ORSClient, ORSConfig
from modules.addressing.resolver import resolve_point
from modules.cabotage.ports_index import load_ports
from modules.cabotage.ports_nearest import find_nearest_port
from modules.cabotage.sea_matrix import SeaMatrix
from modules.road.emissions import estimate_road_trip, TRUCK_SPECS
from modules.cabotage import accounting as acc

# ────────────────────────────────────────────────────────────────────────────────
# Defaults (override via CLI)
# ────────────────────────────────────────────────────────────────────────────────
DIESEL_DENSITY_KG_PER_L: float = 0.84         # 0.83–0.85 common range
DEFAULT_TRUCK: str = "semi_27t"               # see TRUCK_SPECS
DEFAULT_EMPTY_BACKHAUL: float = 0.0           # single-trip door→door

# Sea fuel intensity (kg fuel per tonne-km)
DEFAULT_SEA_K_KG_PER_TKM: float = 0.0027

# Marine gasoil (MGO) price [BRL per tonne]
DEFAULT_MGO_PRICE_BRL_PER_T: float = 3200.0

# Tailpipe emission factors per tonne of MGO burned (TtW) – simplified
EF_TTW_MGO_KG_PER_T = {
      "CO2": 3206.0   # kg CO2 / t fuel
    , "CH4": 0.0
    , "N2O": 0.0
}
GWP100 = { "CH4": 29.8, "N2O": 273.0 }

# Data paths (adjust if your repo layout differs)
DATA_DIR = Path("modules") / "cabotage" / "_data"
PORTS_JSON_PATH = DATA_DIR / "ports_br.json"
SEA_MATRIX_JSON_PATH = DATA_DIR / "sea_matrix.json"
HOTEL_JSON_PATH = DATA_DIR / "hotel.json"    # produced by your calcs/hotel.py pipeline

LOG = logging.getLogger("heatmap.build")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _fmt_tons(x: float) -> str:
    return f"{x:,.0f}t".replace(",", ".")

def _get_gate_point(p: Dict[str, Any]) -> Dict[str, float]:
    """Return the best road-access point for the given port dict (gate if available)."""
    g = p.get("gate")
    if isinstance(g, dict) and "lat" in g and "lon" in g:
        return { "lat": float(g["lat"]), "lon": float(g["lon"]), "label": f"{p['name']} gate" }
    return { "lat": float(p["lat"]), "lon": float(p["lon"]), "label": p["name"] }

def _road_totals_for_distance(
      *
    , distance_km: float
    , cargo_t: float
    , diesel_price_brl_per_l: float
    , truck_key: str
    , empty_backhaul_share: float
) -> Tuple[float, float, float]:
    """Compute (fuel_kg, co2e_kg, cost_brl) for a road movement of *distance_km*."""
    spec = TRUCK_SPECS[truck_key]
    est = estimate_road_trip(
          distance_km=distance_km
        , cargo_t=cargo_t
        , diesel_price_brl_per_l=diesel_price_brl_per_l
        , spec=spec
        , empty_backhaul_share=empty_backhaul_share
    )
    liters = float(est["fuel"]["liters_total"])
    fuel_kg = liters * DIESEL_DENSITY_KG_PER_L
    co2e_kg = float(est["emissions"]["co2e_total_kg"])
    cost_brl = float(est["cost"]["fuel_cost_brl"])
    return fuel_kg, co2e_kg, cost_brl

def _emissions_co2e_from_fuel(
      *
    , fuel_kg: float
    , ef_ttw_per_tonne_fuel: Dict[str, float] = EF_TTW_MGO_KG_PER_T
    , gwp100: Optional[Dict[str, float]] = GWP100
) -> float:
    res = acc.emissions_ttw(
          fuel_kg=fuel_kg
        , ef_ttw_per_tonne_fuel=ef_ttw_per_tonne_fuel
        , gwp100=gwp100
    )
    return float(res.get("CO2e", 0.0))

def _sea_fuel_for_leg(
      *
    , sea_km: float
    , cargo_t: float
    , K_kg_per_tkm: float
) -> float:
    """Simple proportional model: fuel = K * tkm."""
    return float(K_kg_per_tkm) * float(cargo_t) * float(sea_km)

def _port_and_hotel_fuel(
      *
    , origin_port: Dict[str, Any]
    , dest_port: Dict[str, Any]
    , cargo_t: float
    , hotel_json_path: Path
    , K_port_kg_per_t: float = 0.48
    , default_hotel_kg_per_t: float = 0.0
) -> Tuple[float, Dict[str, float]]:
    """Compute fuel for port handling (load+discharge) and hotel-at-berth for the two ports.
    Returns (fuel_total_kg, breakdown)."""
    # Port handling (load + discharge of the shipment)
    f_port = (2.0 * float(cargo_t) * float(K_port_kg_per_t))

    # Hotel factors per city
    hotel = acc.load_hotel_entries(path=str(hotel_json_path))
    idx   = acc.build_hotel_factor_index(hotel_data=hotel)

    k_o = float(idx.get(str(origin_port.get("city", "")).strip(), default_hotel_kg_per_t))
    k_d = float(idx.get(str(dest_port.get("city", "")).strip(), default_hotel_kg_per_t))

    f_hotel = float(cargo_t) * (k_o + k_d)

    return f_port + f_hotel, {
          "port_handling_kg": f_port
        , "hotel_o_kg": float(cargo_t) * k_o
        , "hotel_d_kg": float(cargo_t) * k_d
    }

# ────────────────────────────────────────────────────────────────────────────────
# ORS routing helpers with driving-car fallback
# ────────────────────────────────────────────────────────────────────────────────
def _route_km(ors: ORSClient, src: Dict[str, Any], dst: Dict[str, Any], profile: str) -> float:
    """Call ORS and return distance in km for a given profile."""
    data = ors.route_road(src, dst, profile=profile)  # ORS client supports profile kwarg
    return float(data["distance_m"]) / 1000.0

def _route_km_with_fallback(
      ors: ORSClient
    , src: Dict[str, Any]
    , dst: Dict[str, Any]
    , primary_profile: str = "driving-hgv"
    , fallback_to_car: bool = False
) -> Tuple[float, str]:
    """
    Try ORS with primary profile; optionally fallback to 'driving-car' if any exception occurs.
    Returns (distance_km, used_profile).
    """
    try:
        km = _route_km(ors, src, dst, primary_profile)
        return km, primary_profile
    except Exception as e_primary:
        if fallback_to_car and primary_profile != "driving-car":
            LOG.warning("Primary '%s' failed (%s). Falling back to 'driving-car'.", primary_profile, e_primary)
            km = _route_km(ors, src, dst, "driving-car")
            return km, "driving-car"
        # bubble up original error if no fallback
        raise

# ────────────────────────────────────────────────────────────────────────────────
# Core per-destination computation
# ────────────────────────────────────────────────────────────────────────────────
def compute_for_destination(
      *
    , ors: ORSClient
    , sea_mx: SeaMatrix
    , ports: List[Dict[str, Any]]
    , origin_input: Any
    , dest_input: Any
    , cargo_t: float
    , diesel_price_brl_per_l: float
    , truck_key: str
    , empty_backhaul_share: float
    , K_sea_kg_per_tkm: float
    , mgo_price_brl_per_t: float
    , hotel_json_path: Path
    , include_geo: bool = False
    , ors_profile: str = "driving-hgv"
    , fallback_to_car: bool = False
) -> Dict[str, Any]:
    """Returns a row dict for the CSV with required fields and optional geos."""
    # Resolve endpoints
    o = resolve_point(origin_input, ors=ors)
    d = resolve_point(dest_input,   ors=ors)

    # ROAD ONLY (direct O→D) with fallback
    road_km, used_prof_road = _route_km_with_fallback(
        ors, o, d, primary_profile=ors_profile, fallback_to_car=fallback_to_car
    )
    road_fuel_kg, road_co2e_kg, road_cost_brl = _road_totals_for_distance(
          distance_km=road_km
        , cargo_t=cargo_t
        , diesel_price_brl_per_l=diesel_price_brl_per_l
        , truck_key=truck_key
        , empty_backhaul_share=empty_backhaul_share
    )

    # CABOTAGE: pick nearest ports (gate-aware)
    p_o = find_nearest_port(float(o["lat"]), float(o["lon"]), ports)
    p_d = find_nearest_port(float(d["lat"]), float(d["lon"]), ports)

    gate_o = _get_gate_point(p_o)
    gate_d = _get_gate_point(p_d)

    # Road legs: O→Po and Pd→D with fallback
    km1, used_prof_km1 = _route_km_with_fallback(
        ors, o, gate_o, primary_profile=ors_profile, fallback_to_car=fallback_to_car
    )
    km3, used_prof_km3 = _route_km_with_fallback(
        ors, gate_d, d, primary_profile=ors_profile, fallback_to_car=fallback_to_car
    )

    f1_kg, e1_kg, c1_brl = _road_totals_for_distance(
          distance_km=km1
        , cargo_t=cargo_t
        , diesel_price_brl_per_l=diesel_price_brl_per_l
        , truck_key=truck_key
        , empty_backhaul_share=empty_backhaul_share
    )
    f3_kg, e3_kg, c3_brl = _road_totals_for_distance(
          distance_km=km3
        , cargo_t=cargo_t
        , diesel_price_brl_per_l=diesel_price_brl_per_l
        , truck_key=truck_key
        , empty_backhaul_share=empty_backhaul_share
    )

    # Sea leg distance
    sea_km = float(sea_mx.km(
          { "name": p_o["name"], "lat": float(p_o["lat"]), "lon": float(p_o["lon"]) }
        , { "name": p_d["name"], "lat": float(p_d["lat"]), "lon": float(p_d["lon"]) }
    ))

    fuel_sea_kg = _sea_fuel_for_leg(
          sea_km=sea_km
        , cargo_t=cargo_t
        , K_kg_per_tkm=K_sea_kg_per_tkm
    )
    emis_sea_kg = _emissions_co2e_from_fuel(fuel_kg=fuel_sea_kg)
    cost_sea_brl = (fuel_sea_kg / 1000.0) * mgo_price_brl_per_t

    # Port handling + hotel
    fuel_ops_hotel_kg, _ops_break = _port_and_hotel_fuel(
          origin_port=p_o
        , dest_port=p_d
        , cargo_t=cargo_t
        , hotel_json_path=hotel_json_path
    )
    emis_ops_hotel_kg = _emissions_co2e_from_fuel(fuel_kg=fuel_ops_hotel_kg)
    cost_ops_hotel_brl = (fuel_ops_hotel_kg / 1000.0) * mgo_price_brl_per_t

    # Totals – cabotage
    cab_fuel_kg = f1_kg + fuel_sea_kg + fuel_ops_hotel_kg + f3_kg
    cab_co2e_kg = e1_kg + emis_sea_kg + emis_ops_hotel_kg + e3_kg
    cab_cost_brl = c1_brl + cost_sea_brl + cost_ops_hotel_brl + c3_brl

    # Deltas (cabotage − road direct)
    delta_fuel_kg = cab_fuel_kg - road_fuel_kg
    delta_cost_brl = cab_cost_brl - road_cost_brl
    delta_co2e_kg = cab_co2e_kg - road_co2e_kg

    row = {
          "destiny": str(d.get("label") or dest_input)
        , "delta_fuel_kg": delta_fuel_kg
        , "delta_fuel_cost_brl": delta_cost_brl
        , "delta_co2e_kg": delta_co2e_kg
        , "_profiles_used": f"road={used_prof_road}; o->Po={used_prof_km1}; Pd->d={used_prof_km3}"
    }
    if include_geo:
        row.update({
              "destiny_lat": float(d["lat"])
            , "destiny_lon": float(d["lon"])
        })
    return row

# ────────────────────────────────────────────────────────────────────────────────
# IO
# ────────────────────────────────────────────────────────────────────────────────
def load_destinations(
      dests: Optional[List[str]] = None
    , dest_file: Optional[Path] = None
) -> List[str]:
    if dests and dest_file:
        raise ValueError("Provide either --dest or --dest-file, not both.")
    out: List[str] = []
    if dests:
        out = [x.strip() for x in dests if str(x).strip()]
    elif dest_file:
        txt = Path(dest_file).read_text(encoding="utf-8")
        out = [line.strip() for line in txt.splitlines() if line.strip() and not line.strip().startswith("#")]
    else:
        raise ValueError("No destinations provided. Use --dest or --dest-file.")
    if not out:
        raise ValueError("No usable destinations found.")
    return out

def write_csv(rows: List[Dict[str, Any]], out_path: Path) -> None:
    if not rows:
        raise ValueError("No rows to write.")
    cols = list(rows[0].keys())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Build heatmap CSV of deltas: cabotage − road (negative = saving).")
    p.add_argument("--origin", required=True, help="Origin (city/address/CEP/'lat,lon').")
    p.add_argument("--dest", nargs="+", help="One or more destinations (space-separated).")
    p.add_argument("--dest-file", type=Path, help="Text file with one destination per line.")
    p.add_argument("--amount-tons", type=float, required=True, help="Cargo mass in tonnes.")
    p.add_argument("--outdir", type=Path, default=Path("outputs"), help="Output directory.")
    p.add_argument("--truck", default=DEFAULT_TRUCK, choices=sorted(TRUCK_SPECS.keys()), help="Truck preset for road legs.")
    p.add_argument("--diesel-price", type=float, default=6.0, help="Diesel price [BRL/L] for road legs.")
    p.add_argument("--empty-backhaul", type=float, default=DEFAULT_EMPTY_BACKHAUL, help="Empty backhaul share for road legs (0..1).")
    p.add_argument("--sea-K", type=float, default=DEFAULT_SEA_K_KG_PER_TKM, help="Sea K (kg fuel per t·km).")
    p.add_argument("--mgo-price", type=float, default=DEFAULT_MGO_PRICE_BRL_PER_T, help="Marine fuel price [BRL/t].")
    p.add_argument("--with-geo", action="store_true", help="Include destiny_lat/destiny_lon columns.")
    p.add_argument("--ports-json", type=Path, default=PORTS_JSON_PATH, help="Path to ports_br.json.")
    p.add_argument("--sea-matrix", type=Path, default=SEA_MATRIX_JSON_PATH, help="Path to sea_matrix.json.")
    p.add_argument("--hotel-json", type=Path, default=HOTEL_JSON_PATH, help="Path to hotel.json.")
    # NEW: profile + fallback (no sleeps)
    p.add_argument("--ors-profile", default="driving-hgv", choices=["driving-hgv", "driving-car"],
                   help="Primary ORS routing profile for road legs.")
    p.add_argument("--fallback-to-car", action="store_true",
                   help="If routing with the primary profile fails, retry with 'driving-car'.")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    LOG.info("Origin=%s | amount=%s | truck=%s | profile=%s | fallback_to_car=%s",
             args.origin, _fmt_tons(args.amount_tons), args.truck, args.ors_profile, args.fallback_to_car)

    # ORS client (needs ORS_API_KEY via env)
    cfg = ORSConfig()
    ors = ORSClient(cfg=cfg)

    # Load data
    ports = load_ports(path=str(args.ports_json))
    sea_mx = SeaMatrix.from_json_path(args.sea_matrix)

    # Load destinations
    D = load_destinations(dests=args.dest, dest_file=args.dest_file)
    LOG.info("Destinations: %d", len(D))

    rows: List[Dict[str, Any]] = []
    for i, dest in enumerate(D, start=1):
        try:
            LOG.info("→ [%d/%d] %s", i, len(D), dest)
            row = compute_for_destination(
                  ors=ors
                , sea_mx=sea_mx
                , ports=ports
                , origin_input=args.origin
                , dest_input=dest
                , cargo_t=args.amount_tons
                , diesel_price_brl_per_l=args.diesel_price
                , truck_key=args.truck
                , empty_backhaul_share=args.empty_backhaul
                , K_sea_kg_per_tkm=args.sea_K
                , mgo_price_brl_per_t=args.mgo_price
                , hotel_json_path=args.hotel_json
                , include_geo=args.with_geo
                , ors_profile=args.ors_profile
                , fallback_to_car=args.fallback_to_car
            )
            rows.append(row)
        except Exception as e:
            LOG.error("Failed for destination '%s': %s", dest, e)

    # Write CSV
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / f"{args.origin}__{int(round(args.amount_tons))}t.csv"
    write_csv(rows, out_path)
    LOG.info("Done → %s (rows=%d)", out_path, len(rows))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
