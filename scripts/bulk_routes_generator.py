#!/usr/bin/env python3
# scripts/bulk_routes_generator.py
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
import logging
from typing import Any, List
import importlib.util

from modules.functions._logging import init_logging
from modules.road.ors_common import RateLimited


log = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Loop over many destinations and call routes_generator.py for each.\n"
            "Stops when ORS rate limit (429) is hit. Safe to re-run: already-ingested\n"
            "rows are skipped inside routes_generator."
        )
    )

    p.add_argument(
        "--origin",
        required=True,
        help="Origin (address/city/CEP/'lat,lon').",
    )

    p.add_argument(
        "--dests-file",
        type=Path,
        required=True,
        help="Text file with one destination per line (e.g. 'City, UF').",
    )

    p.add_argument(
        "--ors-profile",
        default="driving-hgv",
        choices=["driving-hgv", "driving-car"],
        help="Primary ORS routing profile for all calls. Default: driving-hgv.",
    )

    try:
        from argparse import BooleanOptionalAction

        p.add_argument(
            "--fallback-to-car",
            default=True,
            action=BooleanOptionalAction,
            help="Retry with driving-car if primary fails. Default: True.",
        )
        p.add_argument(
            "--overwrite",
            default=False,
            action=BooleanOptionalAction,
            help="If True, force recompute in DB even if row exists.",
        )
    except Exception:
        p.add_argument(
            "--fallback-to-car",
            dest="fallback_to_car",
            action="store_true",
            default=True,
        )
        p.add_argument(
            "--no-fallback-to-car",
            dest="fallback_to_car",
            action="store_false",
        )
        p.add_argument(
            "--overwrite",
            dest="overwrite",
            action="store_true",
            default=False,
        )
        p.add_argument(
            "--no-overwrite",
            dest="overwrite",
            action="store_false",
        )

    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level for this bulk script and inner runs.",
    )

    return p


def _load_destinations(path: Path) -> List[str]:
    """Load destinations from a text file, skipping blanks and comment lines."""
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


def _load_routes_generator_module() -> Any:
    """Load scripts/routes_generator.py as a module via its file path."""
    script_path = ROOT / "scripts" / "routes_generator.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"routes_generator.py not found at {script_path}")

    spec = importlib.util.spec_from_file_location("routes_generator_mod", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load routes_generator module spec")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[arg-type]
    return mod


def _build_child_argv(args: Any, destiny: str) -> list[str]:
    """Build argv for a single call to routes_generator.main()."""
    child: list[str] = [
        "--origin",
        args.origin,
        "--destiny",
        destiny,
        "--ors-profile",
        args.ors_profile,
        "--log-level",
        args.log_level,
    ]

    if getattr(args, "fallback_to_car", True):
        child.append("--fallback-to-car")
    else:
        child.append("--no-fallback-to-car")

    if args.overwrite:
        child.append("--overwrite")
    else:
        child.append("--no-overwrite")

    return child


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    init_logging(level=args.log_level, force=True, write_output=False)
    log.info(
        "Bulk routes generator starting for origin=%r, dests_file=%s",
        args.origin,
        args.dests_file,
    )

    dests = _load_destinations(args.dests_file)
    if not dests:
        log.warning("No destinations found in %s (after filtering). Nothing to do.", args.dests_file)
        return 0

    log.info("Loaded %d destinations from %s", len(dests), args.dests_file)

    rg = _load_routes_generator_module()
    processed = 0

    for idx, destiny in enumerate(dests):
        if destiny == args.origin:
            log.info("Skipping idx=%d: destiny matches origin (%r).", idx, destiny)
            continue

        log.info(
            "[%d/%d] idx=%d → destiny=%r — starting routes_generator.main()",
            processed + 1,
            len(dests),
            idx,
            destiny,
        )

        child_argv = _build_child_argv(args, destiny)

        try:
            rc = rg.main(child_argv)
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
            log.warning(
                "routes_generator.main() returned non-zero exit code %d for idx=%d destiny=%r",
                rc,
                idx,
                destiny,
            )

        processed += 1

    log.info(
        "Bulk routes generator finished. Processed %d destinations.",
        processed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
