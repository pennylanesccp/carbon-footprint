#!/usr/bin/env python3
# scripts/single_evaluation.py
# -*- coding: utf-8 -*-

from __future__ import annotations

# --- path bootstrap (must be the first lines of the file) ---
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]  # repo root (one level above /scripts)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ------------------------------------------------------------

import argparse
import json
import logging
from pathlib import Path

from modules.app.evaluator import (
      _evaluate
    , Dependencies
    , DataPaths
    , DEFAULT_SEA_K_KG_PER_TKM
    , DEFAULT_MGO_PRICE_BRL_PER_T
)
from modules.road.emissions import TRUCK_SPECS

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Evaluate ROAD vs CABOTAGE for a single destiny and print JSON."
    )
    p.add_argument("--origin", required=True, help="Origin (address/city/CEP/'lat,lon').")
    p.add_argument("--destiny", required=True, help="Destiny (address/city/CEP/'lat,lon').")
    p.add_argument("--amount-tons", type=float, required=True, help="Cargo mass in tonnes.")

    # Defaults you requested
    p.add_argument(
          "--truck"
        , default="semi_27t"
        , choices=sorted(TRUCK_SPECS.keys())
        , help="Truck preset for road legs. Default: semi_27t"
    )
    p.add_argument(
          "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS routing profile. Default: driving-hgv"
    )

    # Python 3.9+: BooleanOptionalAction → supports --fallback-to-car / --no-fallback-to-car
    try:
        from argparse import BooleanOptionalAction
        p.add_argument(
              "--fallback-to-car"
            , default=True
            , action=BooleanOptionalAction
            , help="Retry with driving-car if primary fails. Default: True"
        )
    except Exception:
        # Fallback for very old Python: still default to True; provide --no-fallback-to-car to disable.
        p.add_argument("--fallback-to-car", dest="fallback_to_car", action="store_true", default=True)
        p.add_argument("--no-fallback-to-car", dest="fallback_to_car", action="store_false")

    # Prices / factors
    p.add_argument("--diesel-price", type=float, default=6.0, help="Diesel price [BRL/L]. Default: 6.0")
    p.add_argument("--empty-backhaul", type=float, default=0.0, help="Empty backhaul share (0..1). Default: 0.0")
    p.add_argument("--sea-K", type=float, default=DEFAULT_SEA_K_KG_PER_TKM, help=f"Sea K (kg fuel per t·km). Default: {DEFAULT_SEA_K_KG_PER_TKM}")
    p.add_argument("--mgo-price", type=float, default=DEFAULT_MGO_PRICE_BRL_PER_T, help=f"Marine fuel price [BRL/t]. Default: {DEFAULT_MGO_PRICE_BRL_PER_T}")

    # Data paths — defaults to your repo layout
    p.add_argument("--ports-json", type=Path, default=DataPaths().ports_json, help="Path to ports_br.json.")
    p.add_argument("--sea-matrix", type=Path, default=DataPaths().sea_matrix_json, help="Path to sea_matrix.json.")
    p.add_argument("--hotel-json", type=Path, default=DataPaths().hotel_json, help="Path to hotel.json.")

    p.add_argument("--with-geo", action="store_true", help="Include origin/destiny lat/lon in output.")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON.")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    paths = DataPaths(
          ports_json=args.ports_json
        , sea_matrix_json=args.sea_matrix
        , hotel_json=args.hotel_json
    )

    res = _evaluate(
          origin=args.origin
        , destiny=args.destiny
        , cargo_t=args.amount_tons
        , truck_key=args.truck
        , diesel_price_brl_per_l=args.diesel_price
        , empty_backhaul_share=args.empty_backhaul
        , K_sea_kg_per_tkm=args.sea_K
        , mgo_price_brl_per_t=args.mgo_price
        , ors_profile=args.ors_profile
        , fallback_to_car=args.fallback_to_car
        , include_geo=args.with_geo
        , deps=Dependencies()     # lazy-wire via ORSConfig + on-disk data
        , paths=paths
    )

    if args.pretty:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(res, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
