#!/usr/bin/env python3
# scripts/bulk_routes_generator.py
# -*- coding: utf-8 -*-

"""
Bulk runner for routes_generator.py

Given:
  - a single origin
  - a text file with one destiny (destination) per line

This script will:

  1. Loop over all destinies in the file (ignoring blanks and '#' comments).
  2. For each destiny, call routes_generator.main([...]) with the proper argv.
  3. Stop cleanly if an ORS 429 rate limit (RateLimited) is raised.
  4. Be safe to re-run, because routes_generator.py itself skips
     already-ingested rows unless --overwrite is explicitly passed.
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
from typing import Any, List

from modules.infra.logging import init_logging
from modules.road.ors_common import RateLimited


log = logging.getLogger(__name__)


# ───────────────────────────────── parser / CLI ────────────────────────────
def _build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI argument parser for the bulk routes generator.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Loop over many destinations and call routes_generator.py for each.\n"
            "Stops when ORS rate limit (429) is hit. Safe to re-run: already-ingested\n"
            "rows are skipped inside routes_generator."
        )
    )

    parser.add_argument(
        "--origin"
        , required=True
        , help="Origin (address/city/CEP/'lat,lon')."
    )

    parser.add_argument(
        "--dests-file"
        , type=Path
        , required=True
        , help="Text file with one destination per line (e.g. 'City, UF')."
    )

    parser.add_argument(
        "--ors-profile"
        , default="driving-hgv"
        , choices=["driving-hgv", "driving-car"]
        , help="Primary ORS routing profile for all calls. Default: driving-hgv."
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
            , help="If True, force recompute in DB even if row exists."
        )

    except Exception:
        # Backwards-compatible way for older argparse without BooleanOptionalAction
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
        "--log-level"
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
        , help="Logging level for this bulk script and inner runs."
    )

    return parser


# ───────────────────────────────── file helpers ────────────────────────────
def _load_destinations(path: Path) -> List[str]:
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


def _load_routes_generator_module() -> ModuleType:
    """
    Load scripts/routes_generator.py as a Python module via its file path.

    Returns
    -------
    ModuleType
        The loaded module object, expected to expose a `main(argv: list[str]) -> int`.
    """
    script_path = ROOT / "scripts" / "routes_generator.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"routes_generator.py not found at {script_path}")

    spec = importlib.util.spec_from_file_location("routes_generator_mod", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load routes_generator module spec")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[arg-type]
    return mod


# ───────────────────────────── child argv builder ──────────────────────────
def _build_child_argv(args: Any, destiny: str) -> list[str]:
    """
    Build argv for a single call to routes_generator.main().

    Parameters
    ----------
    args : argparse.Namespace
        Parsed bulk CLI arguments.
    destiny : str
        Destination string from the dests file.

    Returns
    -------
    list[str]
        Argument vector to be passed to routes_generator.main().
    """
    child: list[str] = [
        "--origin"
        , args.origin
        , "--destiny"
        , destiny
        , "--ors-profile"
        , args.ors_profile
        , "--log-level"
        , args.log_level
    ]

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

    return child


# ─────────────────────────────────── main logic ────────────────────────────
def main(argv: list[str] | None = None) -> int:
    """
    Entrypoint for the bulk routes generator.

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
        "Bulk routes generator starting for origin=%r, dests_file=%s",
        args.origin,
        args.dests_file,
    )

    dests = _load_destinations(args.dests_file)
    if not dests:
        log.warning(
            "No destinations found in %s (after filtering). Nothing to do.",
            args.dests_file,
        )
        return 0

    log.info("Loaded %d destinations from %s", len(dests), args.dests_file)

    rg = _load_routes_generator_module()

    processed = 0
    failures = 0

    for idx, destiny in enumerate(dests):

        log.info(
            "[%d/%d] idx=%d → destiny=%r — starting routes_generator.main()",
            processed + 1,
            len(dests),
            idx,
            destiny,
        )

        child_argv = _build_child_argv(args, destiny)

        try:
            rc = rg.main(child_argv)  # type: ignore[call-arg]
        except RateLimited as e:
            log.warning(
                "ORS rate limit hit while processing idx=%d destiny=%r. "
                "Stopping bulk run. Details: %s",
                idx,
                destiny,
                e,
            )
            break
        except Exception:
            log.exception(
                "Unexpected error while processing idx=%d destiny=%r. Aborting.",
                idx,
                destiny,
            )
            raise

        if rc != 0:
            failures += 1
            log.warning(
                "routes_generator.main() returned non-zero exit code %d for idx=%d destiny=%r",
                rc,
                idx,
                destiny,
            )

        processed += 1

    log.info(
        "Bulk routes generator finished. Processed %d destinations (failures=%d).",
        processed,
        failures,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
