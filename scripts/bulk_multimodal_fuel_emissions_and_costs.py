#!/usr/bin/env python3
# scripts/bulk_multimodal_fuel_emissions_and_costs.py
# -*- coding: utf-8 -*-

"""
Bulk runner for multimodal_fuel_emissions_and_costs.py
======================================================

Given:
  - a single origin
  - a cargo mass (t)
  - a text file with one destiny (destination) per line

This script will:

  1. Loop over all destinies in the file (ignoring blanks and '#' comments).
  2. For each destiny, call scripts.multimodal_fuel_emissions_and_costs.main([...])
     with the proper argv.
  3. Stop cleanly if the child script raises SystemExit with non-zero code
     (e.g. ORS quota / fatal error).
  4. Be safe to re-run, because multimodal_fuel_emissions_and_costs.py itself
     should handle caching / overwrites for routing and results.

Notes
-----
• The child script is responsible for:
    - building multimodal routes
    - computing fuel, emissions and costs
    - persisting anything to SQLite (if implemented there) and/or printing JSON.
• This bulk script just orchestrates the repetition and logging.
"""

from __future__ import annotations

# ───────────────────── path bootstrap (must be first) ─────────────────────
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]  # repo root (one level above /scripts)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ──────────────────────────────────────────────────────────────────────────

import argparse
import logging
import importlib.util
from types import ModuleType
from typing import Any, List, Optional

from modules.infra.logging import init_logging
from modules.infra.database_manager import (
      DEFAULT_DB_PATH
    , DEFAULT_TABLE as DEFAULT_DISTANCE_TABLE
)

log = logging.getLogger(__name__)


# ───────────────────────────────── parser / CLI ────────────────────────────
def _build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI argument parser for the bulk multimodal fuel/emissions/costs runner.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Loop over many destinations and call "
            "multimodal_fuel_emissions_and_costs.py for each.\n"
            "Stops when the child exits with a non-zero status "
            "(e.g. ORS quota exceeded / fatal error).\n"
            "Safe to re-run: caching / overwrite behaviour is handled in the child."
        )
    )

    parser.add_argument(
          "--origin"
        , required=True
        , help="Origin (address/city/CEP/'lat,lon')."
    )

    parser.add_argument(
          "--cargo-t"
        , type=float
        , required=True
        , help="Cargo mass in tonnes (forwarded to each child call)."
    )

    parser.add_argument(
          "--dests-file"
        , type=Path
        , required=True
        , help="Text file with one destination per line (e.g. 'City, UF')."
    )

    # Fuel / truck configuration (forwarded to child)
    parser.add_argument(
          "--truck-key"
        , default="auto_by_weight"
        , help="Truck preset key (child: --truck-key). Default: auto_by_weight."
    )

    parser.add_argument(
          "--diesel-price-override"
        , type=float
        , default=None
        , help="Override diesel price [R$/L] for road legs (child: --diesel-price-override)."
    )

    parser.add_argument(
          "--cabotage-fuel-type"
        , choices=["vlsfo", "mfo"]
        , default="vlsfo"
        , help="Ship fuel type for the sea leg (child: --cabotage-fuel-type)."
    )

    # ORS routing knobs (forwarded to child)
    parser.add_argument(
          "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS profile for all calls. Default: driving-hgv."
    )

    # Boolean flags: prefer BooleanOptionalAction when available (3.9+),
    # but keep a fallback for older Python versions.
    try:
        from argparse import BooleanOptionalAction

        parser.add_argument(
              "--fallback-to-car"
            , default=True
            , action=BooleanOptionalAction
            , help="Retry with driving-car if primary fails. Default: True."
        )
        parser.add_argument(
              "--overwrite"
            , default=False
            , action=BooleanOptionalAction
            , help="If True, force recompute in DB even if legs/results exist."
        )
        parser.add_argument(
              "--include-ops-hotel"
            , dest="include_ops_and_hotel"
            , default=True
            , action=BooleanOptionalAction
            , help="Include port ops + hotel fuel in cabotage leg. Default: True."
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
        parser.add_argument(
              "--overwrite"
            , dest="overwrite"
            , action="store_true"
            , default=False
        )
        parser.add_argument(
              "--no-overwrite"
            , dest="overwrite"
            , action="store_false"
        )
        parser.add_argument(
              "--include-ops-hotel"
            , dest="include_ops_and_hotel"
            , action="store_true"
            , default=True
        )
        parser.add_argument(
              "--no-include-ops-hotel"
            , dest="include_ops_and_hotel"
            , action="store_false"
        )

    # DB / asset paths (forwarded to child)
    parser.add_argument(
          "--db-path"
        , type=Path
        , default=DEFAULT_DB_PATH
        , help=f"SQLite path for routing cache / results. Default: {DEFAULT_DB_PATH}"
    )

    parser.add_argument(
          "--distance-table"
        , default=DEFAULT_DISTANCE_TABLE
        , help=f"Routes cache table name (child: --distance-table). Default: {DEFAULT_DISTANCE_TABLE}"
    )

    parser.add_argument(
          "--ports-json"
        , type=Path
        , default=None
        , help="Override ports JSON path for the child (--ports-json)."
    )

    parser.add_argument(
          "--sea-matrix-json"
        , type=Path
        , default=None
        , help="Override sea matrix JSON path for the child (--sea-matrix-json)."
    )

    parser.add_argument(
          "--hotel-json"
        , type=Path
        , default=None
        , help="Override hotel JSON path for the child (--hotel-json)."
    )

    # Pretty-printing / logging
    parser.add_argument(
          "--pretty"
        , action="store_true"
        , help="Ask the child to pretty-print JSON output (mostly for debugging / small batches)."
    )

    parser.add_argument(
          "--log-level"
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
        , help="Logging level for this bulk script and inner runs."
    )

    return parser


# ───────────────────────────────── file helpers ────────────────────────────
def _load_destinations(
    path: Path
) -> List[str]:
    """
    Load destinations from a text file, skipping:
      - empty lines
      - lines starting with '#'
    """
    if not path.is_file():
        raise FileNotFoundError(f"dests-file not found: {path}")

    dests: List[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        dests.append(line)

    return dests


def _load_multimodal_fuel_module() -> ModuleType:
    """
    Load scripts/multimodal_fuel_emissions_and_costs.py as a Python module via its file path.

    Returns
    -------
    ModuleType
        The loaded module object, expected to expose a `main(argv: list[str]) -> int`.
    """
    script_path = ROOT / "scripts" / "multimodal_fuel_emissions_and_costs.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"multimodal_fuel_emissions_and_costs.py not found at {script_path}")

    spec = importlib.util.spec_from_file_location(
          "multimodal_fuel_emissions_and_costs_mod"
        , script_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load multimodal_fuel_emissions_and_costs module spec")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[arg-type]
    return mod


# ───────────────────────────── child argv builder ──────────────────────────
def _build_child_argv(
    args: Any
    , destiny: str
) -> list[str]:
    """
    Build argv for a single call to multimodal_fuel_emissions_and_costs.main().

    Parameters
    ----------
    args : argparse.Namespace
        Parsed bulk CLI arguments.
    destiny : str
        Destination string from the dests file.

    Returns
    -------
    list[str]
        Argument vector to be passed to multimodal_fuel_emissions_and_costs.main().
    """
    child: list[str] = [
          "--origin"
        , args.origin
        , "--destiny"
        , destiny
        , "--cargo-t"
        , str(args.cargo_t)
        , "--truck-key"
        , args.truck_key
        , "--cabotage-fuel-type"
        , args.cabotage_fuel_type
        , "--ors-profile"
        , args.ors_profile
        , "--db-path"
        , str(args.db_path)
        , "--distance-table"
        , args.distance_table
        , "--log-level"
        , args.log_level
    ]

    # Optional diesel override
    if args.diesel_price_override is not None:
        child.extend([
              "--diesel-price-override"
            , str(args.diesel_price_override)
        ])

    # Fallback profile flag
    if getattr(args, "fallback_to_car", True):
        child.append("--fallback-to-car")
    else:
        child.append("--no-fallback-to-car")

    # Overwrite flag
    if args.overwrite:
        child.append("--overwrite")
    else:
        child.append("--no-overwrite")

    # Include / exclude ops + hotel fuel
    if getattr(args, "include_ops_and_hotel", True):
        child.append("--include-ops-hotel")
    else:
        child.append("--no-include-ops-hotel")

    # Optional asset paths
    if args.ports_json is not None:
        child.extend([
              "--ports-json"
            , str(args.ports_json)
        ])

    if args.sea_matrix_json is not None:
        child.extend([
              "--sea-matrix-json"
            , str(args.sea_matrix_json)
        ])

    if args.hotel_json is not None:
        child.extend([
              "--hotel-json"
            , str(args.hotel_json)
        ])

    # Pretty-print JSON from child
    if args.pretty:
        child.append("--pretty")

    return child


# ─────────────────────────────────── main logic ────────────────────────────
def main(
    argv: Optional[List[str]] = None
) -> int:
    """
    Entrypoint for the bulk multimodal fuel/emissions/costs runner.

    Parameters
    ----------
    argv : list[str] | None
        Optional argument vector (for tests). If None, uses sys.argv[1:].

    Returns
    -------
    int
        Exit code (0 on success).
    """
    args = _build_parser().parse_args(argv)

    # Initialize logging for this script and inner runs
    init_logging(
          level=args.log_level
        , force=True
        , write_output=False
    )

    log.info(
          "Bulk multimodal fuel/emissions/costs starting for "
          "origin=%r, cargo_t=%.3f, dests_file=%s"
        , args.origin
        , args.cargo_t
        , args.dests_file
    )

    dests = _load_destinations(args.dests_file)
    if not dests:
        log.warning(
              "No destinations found in %s (after filtering). Nothing to do."
            , args.dests_file
        )
        return 0

    log.info(
          "Loaded %d destinations from %s"
        , len(dests)
        , args.dests_file
    )

    mf_mod = _load_multimodal_fuel_module()

    processed = 0
    failures = 0

    for idx, destiny in enumerate(dests):
        log.info(
              "[%d/%d] idx=%d → destiny=%r — starting multimodal_fuel_emissions_and_costs.main()"
            , processed + 1
            , len(dests)
            , idx
            , destiny
        )

        child_argv = _build_child_argv(args, destiny)

        try:
            # multimodal_fuel_emissions_and_costs.main() returns an int on success,
            # but may raise SystemExit on fatal conditions (e.g. ORS quota).
            rc = mf_mod.main(child_argv)  # type: ignore[call-arg]
        except SystemExit as e:  # e.code may carry the exit status
            code = e.code if isinstance(e.code, int) else 1
            if code != 0:
                failures += 1
            log.warning(
                  "multimodal_fuel_emissions_and_costs exited via SystemExit(code=%s) "
                  "for idx=%d destiny=%r. Stopping bulk run."
                , code
                , idx
                , destiny
            )
            break
        except Exception:
            log.exception(
                  "Unexpected error while processing idx=%d destiny=%r. Aborting."
                , idx
                , destiny
            )
            raise

        if rc != 0:
            failures += 1
            log.warning(
                  "multimodal_fuel_emissions_and_costs.main() returned non-zero "
                  "exit code %d for idx=%d destiny=%r"
                , rc
                , idx
                , destiny
            )

        processed += 1

    log.info(
          "Bulk multimodal fuel/emissions/costs finished. "
          "Processed %d destinations (failures=%d)."
        , processed
        , failures
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
