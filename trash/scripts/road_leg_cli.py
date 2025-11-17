#!/usr/bin/env python3
# scripts/road_leg_cli.py
# -*- coding: utf-8 -*-

"""
Road leg CLI (thin wrapper around modules.road.router)
======================================================

Purpose
-------
Small, explicit entrypoint to compute a *single* road-only O→D leg and persist
it to the SQLite cache, delegating all core logic to:

    modules.road.router.main(argv)

This replaces the older, overloaded "routes_generator" script.

Usage
-----
Exactly the same CLI as ``python -m modules.road.router``. For example:

    python scripts/road_leg_cli.py \
        --origin  "Avenida Professor Luciano Gualberto, São Paulo" \
        --destiny "Fortaleza, CE" \
        --ors-profile driving-hgv \
        --db-path data/database/carbon_footprint.sqlite \
        --table routes \
        --pretty

All caching, geocoding, ORS routing and error handling (including:
- unresolved addresses → store raw origin/destiny with NULL coords/distance
- NoRoute / partial geocode → store names to avoid re-hitting the API
- hard failures like “Quota Exceeded” → stop immediately

is implemented in ``modules.road.router``.
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────────────────────
# Path bootstrap (must be first)
# ────────────────────────────────────────────────────────────────────────────────
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]  # repo root (one level above /scripts)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ────────────────────────────────────────────────────────────────────────────────
# Delegate to modules.road.router
# ────────────────────────────────────────────────────────────────────────────────
from modules.road.router import main as _road_main


def main(
    argv: list[str] | None = None
) -> int:
    """
    Thin wrapper: forward CLI args directly to modules.road.router.main.

    Parameters
    ----------
    argv : list[str] | None
        If provided, overrides sys.argv[1:]. When None, uses default.

    Returns
    -------
    int
        Exit code from modules.road.router.main.
    """
    return _road_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
