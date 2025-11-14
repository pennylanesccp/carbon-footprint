#!/usr/bin/env python3
# modules/road/router.py
# -*- coding: utf-8 -*-

"""
Precompute *road-only* routing building blocks (generic O→D legs)
and persist them in SQLite as a reusable cache.

Table (see modules.infra.database_manager)
-----------------------------------------
CREATE TABLE IF NOT EXISTS routes (
      origin_name         TEXT      NOT NULL
    , origin_lat          REAL
    , origin_lon          REAL
    , destiny_name        TEXT      NOT NULL
    , destiny_lat         REAL
    , destiny_lon         REAL
    , distance_km         REAL
    , is_hgv              INTEGER   -- 1 = HGV profile, 0 = non-HGV, NULL = unspecified
    , insertion_timestamp TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

Notes
-----
• This is a *generic road legs cache*:
    (origin_name, destiny_name, is_hgv) → distance_km + coordinates.

  It is meant to be reused across:
    - road-only O→D legs
    - cabotage legs (O→Po, Pd→D)
    - any other ORS directions calls.

• Coordinates and distance are allowed to be NULL for geocoding failures
  or placeholder rows ("we tried this pair already").

Caching / failure logic
-----------------------
  • If a row already exists for the given (origin_name, destiny_name)
    (either raw inputs or resolved labels) → skip everything.

  • If an address (CEP / coordinates / free-text) is *not resolvable*:
      - insert a row with the *raw* origin/destiny strings as names
      - lat/lon for the unresolved side = NULL
      - distance_km = NULL
      - is_hgv = NULL
    so this pair is not reprocessed next time.

  • If the API does *not* return a route for resolvable addresses:
      - insert a row with the resolved origin/destiny names
      - lat/lon filled from geocoding
      - distance_km = NULL
      - is_hgv = NULL
    so the pair is not retried again.

  • If the error is due to quota / rate limit (RateLimited / "Quota exceeded"):
      - abort immediately, without inserting a blocking row
        (so the route can be retried later when quota resets).
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────────────────────
# Path bootstrap (must be first)
# ────────────────────────────────────────────────────────────────────────────────
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ────────────────────────────────────────────────────────────────────────────────
# Standard libs
# ────────────────────────────────────────────────────────────────────────────────
import argparse
import json
from typing import Any, Optional, Tuple

# ────────────────────────────────────────────────────────────────────────────────
# Project imports
# ────────────────────────────────────────────────────────────────────────────────
from modules.infra.logging import init_logging, get_logger
from modules.infra.database_manager import (
      db_session
    , ensure_main_table
    , list_runs
    , upsert_run
    , DEFAULT_DB_PATH
    , DEFAULT_TABLE
)
from modules.road.ors_common import ORSConfig, RateLimited, NoRoute
from modules.road.ors_client import ORSClient
from modules.addressing.resolver import resolve_point_null_safe

_log = get_logger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# Small helpers
# ────────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser.

    This router is intentionally simple:
      - receives origin/destiny
      - checks if the leg is already cached
      - otherwise, resolves + routes + persists.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Precompute road-only routing (generic O→D legs) and persist in SQLite."
        )
    )

    # Required spatial inputs
    parser.add_argument(
          "--origin"
        , required=True
        , help="Origin (address/city/CEP/'lat,lon')."
    )
    parser.add_argument(
          "--destiny"
        , required=True
        , help="Destiny (address/city/CEP/'lat,lon')."
    )

    # Routing knobs (profile affects is_hgv flag)
    parser.add_argument(
          "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS routing profile. Default: driving-hgv"
    )

    # Boolean args with fallback for Python <3.9
    try:
        from argparse import BooleanOptionalAction

        parser.add_argument(
              "--fallback-to-car"
            , default=True
            , action=BooleanOptionalAction
            , help="Retry with driving-car if primary fails. Default: True"
        )
    except Exception:
        parser.add_argument(
              "--fallback-to-car"
            , dest="fallback_to_car"
            , action="store_true"
            , default=True
        )
        parser.add_argument(
              "--no-fallback-to-car"
            , dest="fallback_to_car"
            , action="store_false"
        )

    # DB params
    parser.add_argument(
          "--db-path"
        , type=Path
        , default=DEFAULT_DB_PATH
        , help=f"SQLite path. Default: {DEFAULT_DB_PATH}"
    )
    parser.add_argument(
          "--table"
        , default=DEFAULT_TABLE
        , help=f"Target table. Default: {DEFAULT_TABLE}"
    )

    # Output + logging
    parser.add_argument(
          "--pretty"
        , action="store_true"
        , help="Pretty-print JSON."
    )
    parser.add_argument(
          "--log-level"
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )

    return parser


def _has_cached_leg(
      conn
    , origin_key: str
    , destiny_key: str
    , table_name: str
) -> bool:
    """
    Return True if there is *any* row for (origin_key, destiny_key),
    irrespective of is_hgv / distance. Used to avoid reprocessing.
    """
    rows = list_runs(
          conn
        , origin=origin_key
        , destiny=destiny_key
        , table_name=table_name
        , limit=1
    )
    return bool(rows)


def _point_label_lat_lon(
      pt: Any
    , raw_fallback: str
) -> Tuple[str, Optional[float], Optional[float]]:
    """
    Extract (label, lat, lon) from either:
      - GeoPoint-like object (attributes .label, .lat, .lon), or
      - dict with keys 'label', 'lat', 'lon', or
      - None → falls back to raw string and NULL coords.
    """
    if pt is None:
        return str(raw_fallback), None, None

    # label
    label = getattr(pt, "label", None)
    if label is None and isinstance(pt, dict):
        label = pt.get("label")

    # coordinates
    lat = getattr(pt, "lat", None)
    if lat is None and isinstance(pt, dict):
        lat = pt.get("lat")

    lon = getattr(pt, "lon", None)
    if lon is None and isinstance(pt, dict):
        lon = pt.get("lon")

    label_final = str(label) if label is not None else str(raw_fallback)
    lat_f = float(lat) if lat is not None else None
    lon_f = float(lon) if lon is not None else None
    return label_final, lat_f, lon_f


def _route_distance_km(
      ors: ORSClient
    , origin_text: str
    , destiny_text: str
    , primary_profile: str
    , fallback_to_car: bool
) -> Tuple[Optional[str], Optional[float]]:
    """
    Call ORS /v2/directions for origin→destiny.

    Tries:
      1) primary_profile
      2) driving-car (if fallback_to_car and primary_profile != driving-car)

    Behaviour:
      • On success: returns (profile_used, distance_km).
      • On RateLimited: re-raises → caller stops immediately.
      • On NoRoute/other failures: logs and returns (last_profile_tried, None).
    """
    profiles: list[str] = [primary_profile]
    if fallback_to_car and primary_profile != "driving-car":
        profiles.append("driving-car")

    last_exc: Optional[Exception] = None

    for prof in profiles:
        try:
            res = ors.route_road(
                  origin_text
                , destiny_text
                , profile=prof
            )
            dist_m = res.get("distance_m")
            km = None if dist_m is None else float(dist_m) / 1000.0

            _log.info(
                  "ROUTE origin→dest using %s: distance=%s km"
                , prof
                , "NULL" if km is None else f"{km:.3f}"
            )
            return prof, km

        except RateLimited:
            # Bubble up so main() can treat as quota exceeded
            raise

        except NoRoute as exc:
            _log.warning(
                  "NoRoute for profile=%s origin=%r destiny=%r: %s "
                  "→ storing NULL distance for this leg."
                , prof
                , origin_text
                , destiny_text
                , exc
            )
            last_exc = exc

        except Exception as exc:  # noqa: BLE001
            _log.error(
                  "Route failed for profile=%s origin=%r destiny=%r: %s "
                  "→ storing NULL distance for this leg."
                , prof
                , origin_text
                , destiny_text
                , exc
            )
            last_exc = exc

    if last_exc is not None:
        _log.warning(
              "All route attempts failed for origin=%r destiny=%r (profiles_tried=%r). "
              "Distance will be NULL."
            , origin_text
            , destiny_text
            , profiles
        )

    # Return last profile tried (or None) and NULL distance
    return (profiles[-1] if profiles else None), None


# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────

def main(
    argv: Optional[list[str]] = None
) -> int:
    # ------------------------------------------------------------------
    # Parse CLI args + configure logging
    # ------------------------------------------------------------------
    args = _build_parser().parse_args(argv)

    init_logging(level=args.log_level, force=True, write_output=False)
    _log.info(
          "Road router starting for origin=%r destiny=%r [profile=%s]"
        , args.origin
        , args.destiny
        , args.ors_profile
    )

    # ------------------------------------------------------------------
    # 0) Early DB gate on RAW input strings
    # ------------------------------------------------------------------
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)

        if _has_cached_leg(
              conn
            , origin_key=args.origin
            , destiny_key=args.destiny
            , table_name=args.table
        ):
            _log.info(
                  "Cache hit (raw input names) for (%s → %s); skipping geocoding and routing."
                , args.origin
                , args.destiny
            )
            return 0

    # Local ORS client for geocoding + routing
    ors = ORSClient(cfg=ORSConfig())

    # ------------------------------------------------------------------
    # 1) Geocode origin/destiny once (later reused for the leg)
    #    • resolve_point_null_safe may return None → handle gracefully.
    # ------------------------------------------------------------------
    origin_pt = resolve_point_null_safe(
          value=args.origin
        , ors=ors
        , log=_log
    )
    destiny_pt = resolve_point_null_safe(
          value=args.destiny
        , ors=ors
        , log=_log
    )

    origin_name, origin_lat, origin_lon = _point_label_lat_lon(origin_pt, args.origin)
    destiny_name, destiny_lat, destiny_lon = _point_label_lat_lon(destiny_pt, args.destiny)

    # If either side is not resolvable → persist a NULL-distance row keyed by raw/resolved names.
    if origin_pt is None or destiny_pt is None:
        _log.warning(
              "Origin or destiny could not be fully geocoded; caching NULL-distance leg."
              " origin_raw=%r destiny_raw=%r"
            , args.origin
            , args.destiny
        )

        with db_session(db_path=args.db_path) as conn:
            ensure_main_table(conn, table_name=args.table)
            upsert_run(
                  conn
                , origin=origin_name
                , origin_lat=origin_lat
                , origin_lon=origin_lon
                , destiny=destiny_name
                , destiny_lat=destiny_lat
                , destiny_lon=destiny_lon
                , distance_km=None
                , is_hgv=None
                , table_name=args.table
            )

        payload = {
              "origin": {
                  "label": origin_name
                , "lat": origin_lat
                , "lon": origin_lon
            }
            , "destiny": {
                  "label": destiny_name
                , "lat": destiny_lat
                , "lon": destiny_lon
            }
            , "distance_km": None
            , "profile_used": None
            , "status": "geocode_failed"
        }

        if args.pretty:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

        _log.info(
              "Road router finished (geocode failed) for (%s → %s)."
            , origin_name
            , destiny_name
        )
        return 0

    # ------------------------------------------------------------------
    # 2) Second DB gate on RESOLVED names (canonical key)
    # ------------------------------------------------------------------
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)

        if _has_cached_leg(
              conn
            , origin_key=origin_name
            , destiny_key=destiny_name
            , table_name=args.table
        ):
            _log.info(
                  "Cache hit (resolved names) for (%s → %s); skipping routing API calls."
                , origin_name
                , destiny_name
            )
            return 0

    # ------------------------------------------------------------------
    # 3) Main road route O→D (single generic leg)
    # ------------------------------------------------------------------
    try:
        profile_used_road, distance_km = _route_distance_km(
              ors
            , origin_text=args.origin
            , destiny_text=args.destiny
            , primary_profile=args.ors_profile
            , fallback_to_car=args.fallback_to_car
        )
    except RateLimited as exc:
        # Quota / rate limit → abort immediately, do NOT insert a blocking row.
        _log.error(
              "ORS quota / rate limit reached (RateLimited): %s — aborting router."
            , exc
        )
        return 2

    # Determine is_hgv only if distance is known
    if distance_km is None:
        is_hgv: Optional[bool] = None
    else:
        if profile_used_road == "driving-hgv":
            is_hgv = True
        elif profile_used_road == "driving-car":
            is_hgv = False
        else:
            is_hgv = None

    _log.info(
          "Extracted geo and distance: origin=%r (%.6f,%.6f) → destiny=%r (%.6f,%.6f) "
          "distance_km=%s is_hgv=%r"
        , origin_name
        , float(origin_lat or 0.0)
        , float(origin_lon or 0.0)
        , destiny_name
        , float(destiny_lat or 0.0)
        , float(destiny_lon or 0.0)
        , "NULL" if distance_km is None else f"{distance_km:.3f}"
        , is_hgv
    )

    # ------------------------------------------------------------------
    # 4) Persist in SQLite (single upsert row)
    # ------------------------------------------------------------------
    with db_session(db_path=args.db_path) as conn:
        ensure_main_table(conn, table_name=args.table)
        upsert_run(
              conn
            , origin=origin_name
            , origin_lat=origin_lat
            , origin_lon=origin_lon
            , destiny=destiny_name
            , destiny_lat=destiny_lat
            , destiny_lon=destiny_lon
            , distance_km=distance_km
            , is_hgv=is_hgv
            , table_name=args.table
        )

    # ------------------------------------------------------------------
    # 5) Optional JSON echo (handy for CI logs / quick checks)
    # ------------------------------------------------------------------
    payload = {
          "origin": {
              "label": origin_name
            , "lat": origin_lat
            , "lon": origin_lon
        }
        , "destiny": {
              "label": destiny_name
            , "lat": destiny_lat
            , "lon": destiny_lon
        }
        , "distance_km": distance_km
        , "profile_used": profile_used_road if distance_km is not None else None
        , "status": "ok" if distance_km is not None else "no_route"
    }

    if args.pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    _log.info(
          "Road router finished for (%s → %s)."
        , origin_name
        , destiny_name
    )
    return 0


# ────────────────────────────────────────────────────────────────────────────────
# CLI smoke test
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys as _sys

    # If no extra CLI args were passed, run a small canned smoke test:
    #   python -m modules.road.router
    #
    # Otherwise, behave like a normal CLI:
    #   python -m modules.road.router --origin "A" --destiny "B" --pretty
    if len(_sys.argv) == 1:
        test_argv = [
              "--origin", "Avenida Professor Luciano Gualberto, São Paulo"
            , "--destiny", "Fortaleza, CE"
            , "--pretty"
        ]
        raise SystemExit(main(test_argv))

    raise SystemExit(main())
