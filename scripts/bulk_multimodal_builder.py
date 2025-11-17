#!/usr/bin/env python3
# scripts/bulk_multimodal_builder.py
# -*- coding: utf-8 -*-

"""
Bulk runner for multimodal_builder.py
=====================================

Given:
  - a single origin
  - a text file with one destiny (destination) per line

This script will:

  1. Loop over all destinies in the file (ignoring blanks and '#' comments).
  2. For each destiny, call scripts.multimodal_builder.main([...]) with the proper argv.
  3. Stop cleanly if the child script raises SystemExit (e.g. ORS quota / fatal error).
  4. Be safe to re-run, because multimodal_builder.py itself:
       - skips cached legs unless --overwrite is passed
       - overwrites legs properly when --overwrite=True.
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

log = logging.getLogger(__name__)


# ───────────────────────────────── parser / CLI ────────────────────────────
def _build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI argument parser for the bulk multimodal builder.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Loop over many destinations and call multimodal_builder.py for each.\n"
            "Stops when the child exits with a non-zero status (e.g. ORS quota exceeded).\n"
            "Safe to re-run: cached legs are handled inside multimodal_builder."
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
            , help="If True, force recompute in DB even if legs exist."
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
          "--pretty"
        , action="store_true"
        , help="Pretty-print child JSON output (mostly for debugging / small batches)."
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


def _load_multimodal_builder_module() -> ModuleType:
    """
    Load scripts/multimodal_builder.py as a Python module via its file path.

    Returns
    -------
    ModuleType
        The loaded module object, expected to expose a `main(argv: list[str]) -> int`.
    """
    script_path = ROOT / "scripts" / "multimodal_builder.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"multimodal_builder.py not found at {script_path}")

    spec = importlib.util.spec_from_file_location("multimodal_builder_mod", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load multimodal_builder module spec")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[arg-type]
    return mod


# ───────────────────────────── child argv builder ──────────────────────────
def _build_child_argv(
    args: Any
    , destiny: str
) -> list[str]:
    """
    Build argv for a single call to multimodal_builder.main().

    Parameters
    ----------
    args : argparse.Namespace
        Parsed bulk CLI arguments.
    destiny : str
        Destination string from the dests file.

    Returns
    -------
    list[str]
        Argument vector to be passed to multimodal_builder.main().
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

    # Pretty-print JSON from child
    if args.pretty:
        child.append("--pretty")

    return child


# ─────────────────────────────────── main logic ────────────────────────────
def main(
    argv: list[str] | None = None
) -> int:
    """
    Entrypoint for the bulk multimodal builder.

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
          "Bulk multimodal builder starting for origin=%r, dests_file=%s"
        , args.origin
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

    mb = _load_multimodal_builder_module()

    processed = 0
    failures = 0

    for idx, destiny in enumerate(dests):
        log.info(
              "[%d/%d] idx=%d → destiny=%r — starting multimodal_builder.main()"
            , processed + 1
            , len(dests)
            , idx
            , destiny
        )

        child_argv = _build_child_argv(args, destiny)

        try:
            # multimodal_builder.main() returns an int on success,
            # but may raise SystemExit(1) on fatal conditions (e.g. ORS quota).
            rc = mb.main(child_argv)  # type: ignore[call-arg]
        except SystemExit as e:  # e.code may carry the exit status
            code = e.code if isinstance(e.code, int) else 1
            failures += 1 if code != 0 else 0
            log.warning(
                  "multimodal_builder exited via SystemExit(code=%s) for idx=%d destiny=%r. "
                  "Stopping bulk run."
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
                  "multimodal_builder.main() returned non-zero exit code %d for idx=%d destiny=%r"
                , rc
                , idx
                , destiny
            )

        processed += 1

    log.info(
          "Bulk multimodal builder finished. Processed %d destinations (failures=%d)."
        , processed
        , failures
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
