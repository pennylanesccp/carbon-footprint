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
from typing import Any, Optional

from modules.infra.logging import init_logging
from modules.app.evaluator import (
      evaluate
    , Dependencies
    , DataPaths
    , DEFAULT_SEA_K_KG_PER_TKM
    , DEFAULT_MGO_PRICE_BRL_PER_T
)
from modules.fuel.truck_specs import TRUCK_SPECS
from modules.fuel.diesel_prices import DEFAULT_DIESEL_PRICES_CSV

# DB utils
from modules.functions.database_manager import (
      db_session
    , ensure_main_table
    , get_run
    , upsert_run
    , DEFAULT_DB_PATH
    , DEFAULT_TABLE
)

log = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Evaluate ROAD vs CABOTAGE for a single destiny and print JSON (with DB caching/overwrite)."
    )
    p.add_argument("--origin", required=True, help="Origin (address/city/CEP/'lat,lon').")
    p.add_argument("--destiny", required=True, help="Destiny (address/city/CEP/'lat,lon').")
    p.add_argument("--amount-tons", type=float, required=True, help="Cargo mass in tonnes.")

    # truck profile
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
        p.add_argument(
              "--overwrite"
            , default=False
            , action=BooleanOptionalAction
            , help="If True, delete existing row and recompute."
        )
    except Exception:  # py<3.9 fallback
        p.add_argument("--fallback-to-car", dest="fallback_to_car", action="store_true", default=True)
        p.add_argument("--no-fallback-to-car", dest="fallback_to_car", action="store_false")
        p.add_argument("--overwrite", dest="overwrite", action="store_true", default=False)
        p.add_argument("--no-overwrite", dest="overwrite", action="store_false")

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

    # DB params
    p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help=f"SQLite path. Default: {DEFAULT_DB_PATH}")
    p.add_argument("--table", default=DEFAULT_TABLE, help=f"Target table. Default: {DEFAULT_TABLE}")

    # UX
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON.")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


# ────────────────────────────────────────────────────────────────────────────────
# Helpers to pull fields from `evaluate` result (robust to minor structure drift)
# ────────────────────────────────────────────────────────────────────────────────

def _get_geo(obj: dict[str, Any], key: str) -> tuple[str, float, float]:
    """
    Return (label, lat, lon) for the role `key` from the evaluation output.

    Tries a bunch of reasonable places/names:
      • top-level:            obj["origin"] / obj["destiny"] / obj["o"] / obj["d"] / ...
      • containers:           obj["geo"][...], obj["locations"][...], obj["inputs"][...], obj["meta"][...], obj["context"][...]
      • flat fields:          origin_lat/origin_lon, destiny_lat/destiny_lon, o_lat/o_lon, d_lat/d_lon
    """
    # alias set for each role
    if key == "origin":
        aliases = ["origin", "o", "orig", "origin_point"]
    elif key == "destiny":
        aliases = ["destiny", "d", "dest", "destiny_point"]
    else:
        aliases = [key]

    def try_node(n: Any) -> Optional[tuple[str, float, float]]:
        if not isinstance(n, dict):
            return None
        lat = n.get("lat") or n.get("latitude")
        lon = n.get("lon") or n.get("lng") or n.get("longitude")
        if lat is None or lon is None:
            return None
        label = n.get("label") or n.get("name") or key
        return str(label), float(lat), float(lon)

    # 1) direct: top-level dicts
    for k in aliases:
        hit = try_node(obj.get(k))
        if hit:
            return hit

    # 2) common containers
    containers = [
        obj.get("geo") or {},
        obj.get("locations") or {},
        obj.get("inputs") or {},
        obj.get("meta") or {},
        obj.get("context") or {},
    ]
    for container in containers:
        if isinstance(container, dict):
            for k in aliases:
                hit = try_node(container.get(k))
                if hit:
                    return hit

    # 3) flat fields, e.g., origin_lat/origin_lon or o_lat/o_lon
    for k in aliases:
        lat = obj.get(f"{k}_lat")
        lon = obj.get(f"{k}_lon") or obj.get(f"{k}_lng")
        if lat is not None and lon is not None:
            label = (
                obj.get(f"{k}_label")
                or obj.get(f"{k}_name")
                or (obj.get(k) if isinstance(obj.get(k), str) else None)
                or key
            )
            return str(label), float(lat), float(lon)

    raise ValueError(f"Missing lat/lon for '{key}' in evaluation output.")

def _get_road_only_distance_km(res: dict[str, Any]) -> Optional[float]:
    if "road_only_distance_km" in res:
        return float(res["road_only_distance_km"]) if res["road_only_distance_km"] is not None else None
    ro = res.get("road_only") or {}
    if isinstance(ro, dict):
        if ro.get("distance_km") is not None:
            return float(ro["distance_km"])
        dist = ro.get("distance") or {}
        if isinstance(dist, dict) and dist.get("km") is not None:
            return float(dist["km"])
    return None


def _get_cabotage_pieces(res: dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[float], Optional[float]]:
    cab = res.get("cabotage") or {}
    if not isinstance(cab, dict):
        return None, None, None, None

    po_name = cab.get("po_name") or (cab.get("po") or {}).get("name")
    pd_name = cab.get("pd_name") or (cab.get("pd") or {}).get("name")

    # road segments around sea
    o_po_km = (
        cab.get("road_o_to_po_km")
        or (cab.get("o_to_po") or {}).get("distance_km")
        or ((cab.get("road") or {}).get("o_to_po") or {}).get("distance_km")
    )
    pd_d_km = (
        cab.get("road_pd_to_d_km")
        or (cab.get("pd_to_d") or {}).get("distance_km")
        or ((cab.get("road") or {}).get("pd_to_d") or {}).get("distance_km")
    )

    o_po_km = None if o_po_km is None else float(o_po_km)
    pd_d_km = None if pd_d_km is None else float(pd_d_km)
    return po_name, pd_name, o_po_km, pd_d_km


def _infer_is_hgv(res: dict[str, Any], primary_profile: str) -> Optional[bool]:
    prof = (
          (res.get("routing") or {}).get("profile_used")
        or res.get("ors_profile_used")
        or primary_profile
    )
    if prof is None:
        return None
    return True if prof == "driving-hgv" else False if prof == "driving-car" else None


def _delete_key(conn, *, table: str, origin_name: str, cargo_weight_ton: float, destiny_name: str) -> None:
    conn.execute(
        f"DELETE FROM {table} WHERE origin_name=? AND cargo_weight_ton=? AND destiny_name=?",
        (origin_name, float(cargo_weight_ton), destiny_name),
    )


# ────────────────────────────────────────────────────────────────────────────────

def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)

    init_logging(level=args.log_level, force=True, write_output=False)

    # Resolve data paths
    paths = DataPaths(
          ports_json=args.ports_json
        , sea_matrix_json=args.sea_matrix
        , hotel_json=args.hotel_json
        , diesel_prices_csv=args.diesel_prices_csv
    )

    # DB existence check
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)

        # The composite key for caching
        key_origin = args.origin
        key_dest   = args.destiny
        key_weight = float(args.amount_tons)

        existing = get_run(
              conn
            , origin_name=key_origin
            , cargo_weight_ton=key_weight
            , destiny_name=key_dest
            , table_name=args.table
        )

        # ── decision matrix ─────────────────────────────────────────────────────
        if not args.overwrite and existing:
            # default mode + already present => do nothing (no print)
            log.debug("Cache hit (overwrite=False). Skipping evaluation.")
            return 0

        if args.overwrite and existing:
            # overwrite mode: delete then recompute/insert
            _delete_key(
                  conn
                , table=args.table
                , origin_name=key_origin
                , cargo_weight_ton=key_weight
                , destiny_name=key_dest
            )
            log.debug("Overwrite=True ⇒ deleted existing row before recompute.")

    # Run evaluation (force include_geo for DB persistence)
    res = evaluate(
          origin=args.origin
        , destiny=args.destiny
        , cargo_t=args.amount_tons
        , truck_key=args.truck
        , diesel_price_brl_per_l=args.diesel_price
        , diesel_prices_csv=args.diesel_prices_csv
        , empty_backhaul_share=args.empty_backhaul
        , K_sea_kg_per_tkm=args.sea_K
        , mgo_price_brl_per_t=args.mgo_price
        , ors_profile=args.ors_profile
        , fallback_to_car=args.fallback_to_car
        , include_geo=True
        , deps=Dependencies()
        , paths=paths
    )

    # Extract fields to persist
    origin_name, origin_lat, origin_lon = _get_geo(res, "origin")
    destiny_name, destiny_lat, destiny_lon = _get_geo(res, "destiny")
    road_only_distance_km = _get_road_only_distance_km(res)
    cab_po_name, cab_pd_name, cab_road_o_to_po_km, cab_road_pd_to_d_km = _get_cabotage_pieces(res)
    is_hgv = _infer_is_hgv(res, args.ors_profile)

    # Persist (upsert is fine; if overwrite we already deleted so this becomes fresh insert)
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)
        upsert_run(
              conn
            , origin_name=origin_name
            , origin_lat=origin_lat
            , origin_lon=origin_lon
            , destiny_name=destiny_name
            , destiny_lat=destiny_lat
            , destiny_lon=destiny_lon
            , cargo_weight_ton=float(args.amount_tons)
            , road_only_distance_km=road_only_distance_km
            , cab_po_name=cab_po_name
            , cab_pd_name=cab_pd_name
            , cab_road_o_to_po_km=cab_road_o_to_po_km
            , cab_road_pd_to_d_km=cab_road_pd_to_d_km
            , is_hgv=is_hgv
            , table_name=args.table
        )

    # Output the evaluation JSON (only when we actually evaluated)
    if args.pretty:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(res, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
