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

from modules.functions.logging import init_logging
from modules.app.evaluator import (
      evaluate
    , Dependencies
    , DataPaths
    , DEFAULT_SEA_K_KG_PER_TKM
    , DEFAULT_MGO_PRICE_BRL_PER_T
)
from modules.road.truck_specs import TRUCK_SPECS
from modules.road.diesel_prices import DEFAULT_DIESEL_PRICES_CSV


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Evaluate ROAD vs CABOTAGE for a single destiny and print JSON."
    )
    p.add_argument("--origin", required=True, help="Origin (address/city/CEP/'lat,lon').")
    p.add_argument("--destiny", required=True, help="Destiny (address/city/CEP/'lat,lon').")
    p.add_argument("--amount-tons", type=float, required=True, help="Cargo mass in tonnes.")

    # Accept presets AND the special auto selector
    truck_choices = sorted(set(TRUCK_SPECS.keys()) | {"auto", "auto_by_weight"})
    p.add_argument(
          "--truck"
        , default="semi_27t"
        , choices=truck_choices
        , help="Truck preset for road legs (e.g., semi_27t) or 'auto_by_weight'. Default: semi_27t"
    )
    p.add_argument(
          "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS routing profile. Default: driving-hgv"
    )

    try:
        from argparse import BooleanOptionalAction
        p.add_argument(
              "--fallback-to-car"
            , default=True
            , action=BooleanOptionalAction
            , help="Retry with driving-car if primary fails. Default: True"
        )
    except Exception:
        p.add_argument("--fallback-to-car", dest="fallback_to_car", action="store_true", default=True)
        p.add_argument("--no-fallback-to-car", dest="fallback_to_car", action="store_false")

    # ── Prices / factors ────────────────────────────────────────────────────────
    p.add_argument(
          "--diesel-price"
        , type=float
        , default=None
        , help="Override diesel price [BRL/L]. If omitted, use CSV average of origin/destiny UF."
    )
    p.add_argument("--empty-backhaul", type=float, default=0.0, help="Empty backhaul share (0..1). Default: 0.0")
    p.add_argument("--sea-K", type=float, default=DEFAULT_SEA_K_KG_PER_TKM, help=f"Sea K (kg fuel per t·km). Default: {DEFAULT_SEA_K_KG_PER_TKM}")
    p.add_argument("--mgo-price", type=float, default=DEFAULT_MGO_PRICE_BRL_PER_T, help=f"Marine fuel price [BRL/t]. Default: {DEFAULT_MGO_PRICE_BRL_PER_T}")

    # ── Data paths ─────────────────────────────────────────────────────────────
    dp = DataPaths()
    p.add_argument("--ports-json", type=Path, default=dp.ports_json, help="Path to ports_br.json.")
    p.add_argument("--sea-matrix", type=Path, default=dp.sea_matrix_json, help="Path to sea_matrix.json.")
    p.add_argument("--hotel-json", type=Path, default=dp.hotel_json, help="Path to hotel.json.")
    p.add_argument(
          "--diesel-prices-csv"
        , type=Path
        , default=dp.diesel_prices_csv
        , help=f"CSV with columns UF,price_brl_l. Default: {dp.diesel_prices_csv} "
               f"(falls back to {DEFAULT_DIESEL_PRICES_CSV})."
    )

    p.add_argument("--with-geo", action="store_true", help="Include origin/destiny lat/lon in output.")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON.")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)

    # Standard project logger (no file write by default)
    init_logging(level=args.log_level, force=True, write_output=False)

    paths = DataPaths(
          ports_json=args.ports_json
        , sea_matrix_json=args.sea_matrix
        , hotel_json=args.hotel_json
        , diesel_prices_csv=args.diesel_prices_csv
    )

    res = evaluate(
          origin=args.origin
        , destiny=args.destiny
        , cargo_t=args.amount_tons
        , truck_key=args.truck
        , diesel_price_brl_per_l=args.diesel_price          # None ⇒ compute from CSV
        , diesel_prices_csv=args.diesel_prices_csv
        , empty_backhaul_share=args.empty_backhaul
        , K_sea_kg_per_tkm=args.sea_K
        , mgo_price_brl_per_t=args.mgo_price
        , ors_profile=args.ors_profile
        , fallback_to_car=args.fallback_to_car
        , include_geo=args.with_geo
        , deps=Dependencies()
        , paths=paths
    )

    if args.pretty:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(res, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
