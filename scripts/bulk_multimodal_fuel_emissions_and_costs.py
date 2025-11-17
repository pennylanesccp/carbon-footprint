#!/usr/bin/env python3
# scripts/bulk_multimodal_fuel_emissions.py
# -*- coding: utf-8 -*-

"""
Bulk multimodal fuel, emissions & costs loader
==============================================

Given:
  - one origin (address/city/CEP/'lat,lon')
  - a cargo mass (t)
  - a text file with one destiny per line

This script will, for **each** destiny:

  1) Call `scripts/multimodal_fuel_emissions.py` as a child process.
  2) Capture its final JSON payload (fuel, costs, emissions).
  3) Upsert a row into a per-origin+cargo table in SQLite.

Table naming
------------

By default, the multimodal results are written to a table whose name is:

    [OriginSanitized]__[AmountInTons]tons

where:
    OriginSanitized  → accents removed, non-alphanum stripped, spaces → '_'
    AmountInTons     → "30tons", "26_5tons", etc.

Examples:
    Sao_Paulo__30tons
    Av_Luciano_Gualberto__26tons

The schema and upsert are handled by:

    modules.infra.database_manager.ensure_multimodal_results_table(...)
    modules.infra.database_manager.upsert_multimodal_result(...)

Usage (PowerShell)
------------------

    # venv active; ORS_API_KEY set
    python scripts/bulk_multimodal_fuel_emissions_and_costs.py `
        --origin "São Paulo, SP" `
        --cargo-t 30 `
        --dest-file data/city_dests.txt `
        --log-level INFO `
        --write-output `
        --resume

Notes
-----

• Destinations are read from a simple text file:
    - blank lines and lines starting with '#' are ignored.
• When --resume is set, destinations already present in the
  multimodal table (same table name) are skipped.
• Child logs are streamed to the console and (optionally) to the
  repo's default log folder. The final JSON is not echoed, only parsed.
• ORS timeout/retry knobs are forwarded via environment variables
  so ORSConfig/ORSClient in the child can pick them up.
"""

from __future__ import annotations

# ── repo path bootstrap (keep first) ────────────────────────────────────────────
from pathlib import Path
import sys as _sys

ROOT = Path(__file__).resolve().parents[1]  # repo root (one level above /scripts)
if str(ROOT) not in _sys.path:
    _sys.path.insert(0, str(ROOT))
# ────────────────────────────────────────────────────────────────────────────────

# Ensure THIS process prints Unicode cleanly on Windows terminals
try:
    _sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    _sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

import argparse
import json
import os
import re
import subprocess
import threading
import time
import unicodedata
from typing import Any, Dict, List, Optional, Set

from modules.infra.logging import (
      init_logging
    , get_logger
    , get_current_log_path
)
from modules.infra.database_manager import (
      db_session
    , ensure_multimodal_results_table
    , upsert_multimodal_result
    , DEFAULT_DB_PATH
)

_LOG = get_logger("scripts.bulk_multimodal_fuel_emissions")  # explicit name

# ────────────────────────────────────────────────────────────────────────────────
# Helpers: sanitization, file reading
# ────────────────────────────────────────────────────────────────────────────────

def _strip_accents_and_sanitize(
    s: str
) -> str:
    """
    Remove accents; keep letters/digits/space/_; collapse spaces to underscores.
    Suitable for table-name fragments.
    """
    n = unicodedata.normalize("NFKD", s)
    n = "".join(ch for ch in n if not unicodedata.combining(ch))
    n = re.sub(r"[^A-Za-z0-9 _]", " ", n)
    n = re.sub(r"\s+", "_", n.strip())
    return n


def _amount_tag(
    tons: float
) -> str:
    """
    Format cargo amount for table name (int → '30tons', else e.g. '26_5tons').
    """
    if abs(tons - round(tons)) < 1e-9:
        return f"{int(round(tons))}tons"
    s = f"{tons:.2f}".rstrip("0").rstrip(".").replace(".", "_")
    return f"{s}tons"


def _read_dest_file(
    path: Path
) -> List[str]:
    """
    Read destinations from a text file, ignoring blanks and '#' comments.
    """
    txt = path.read_text(encoding="utf-8")
    out: List[str] = []
    for line in txt.splitlines():
        t = line.strip()
        if not t or t.startswith("#"):
            continue
        out.append(t)
    if not out:
        raise ValueError(f"No usable destinations found in {path}")
    return out


# ────────────────────────────────────────────────────────────────────────────────
# Helpers: JSON extraction & log streaming
# ────────────────────────────────────────────────────────────────────────────────

def _extract_last_json_object(
    text: str
) -> Dict[str, Any]:
    """
    Child script logs a lot and then prints one final JSON object.

    Strategy:
      1) Try to find the last '{' and parse from there.
      2) Fallback: scan for a balanced {...} block.
    """
    j = text.rfind("{")
    if j != -1:
        candidate = text[j:]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    start = None
    depth = 0
    for i, ch in enumerate(text):
        if ch == "{":
            if start is None:
                start = i
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start is not None:
                    block = text[start : i + 1]
                    try:
                        return json.loads(block)
                    except json.JSONDecodeError:
                        start = None
                        continue
    raise ValueError("Could not parse final JSON from child output")


def _is_log_line(
    s: str
) -> bool:
    """
    Our log lines start with '[' (e.g., "[2025-11-10 09:42:50][INFO][modules…] …").
    The final JSON starts with '{'. Use that to decide what to show.
    """
    return s.lstrip().startswith("[")


def _append_to_file(
    path: Optional[str]
    , text: str
) -> None:
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(text)
    except Exception:
        # don't crash the run if writing the log file fails
        pass


def _forward_timeout_env(
      *
    , connect_timeout_s: Optional[float]
    , read_timeout_fast_s: Optional[float]
    , read_timeout_slow_s: Optional[float]
    , max_retries: Optional[int]
    , backoff_s: Optional[float]
    , escalate_on_timeout: Optional[bool]
) -> Dict[str, str]:
    """
    Build a dict of ORS_* env vars that the child process (ORSConfig/ORSClient)
    can read to tune timeouts/retries. Only set keys for provided values.
    """
    out: Dict[str, str] = {}
    if connect_timeout_s is not None:
        out["ORS_CONNECT_TIMEOUT_S"] = str(connect_timeout_s)
    if read_timeout_fast_s is not None:
        out["ORS_READ_TIMEOUT_FAST_S"] = str(read_timeout_fast_s)
    if read_timeout_slow_s is not None:
        out["ORS_READ_TIMEOUT_SLOW_S"] = str(read_timeout_slow_s)
    if max_retries is not None:
        out["ORS_MAX_RETRIES"] = str(max_retries)
    if backoff_s is not None:
        out["ORS_BACKOFF_S"] = str(backoff_s)
    if escalate_on_timeout is not None:
        out["ORS_ESCALATE_ON_TIMEOUT"] = "1" if escalate_on_timeout else "0"
    return out


# ────────────────────────────────────────────────────────────────────────────────
# Child process runner — call scripts/multimodal_fuel_emissions.py
# ────────────────────────────────────────────────────────────────────────────────

def _run_multimodal_fuel_emissions(
      *
    , origin: str
    , destiny: str
    , cargo_t: float
    , truck_key: Optional[str] = "auto_by_weight"
    , diesel_price_override: Optional[float] = None
    , cabotage_fuel_type: Optional[str] = "vlsfo"
    , ors_profile: Optional[str] = None
    , fallback_to_car: bool = True
    , include_ops_and_hotel: bool = True
    , overwrite_routes: bool = False
    , routes_db_path: Optional[Path] = None
    , routes_table: Optional[str] = None
    , ports_json: Optional[Path] = None
    , sea_matrix_json: Optional[Path] = None
    , hotel_json: Optional[Path] = None
    , log_level: str = "INFO"
    , script_path: Path = Path("scripts") / "multimodal_fuel_emissions.py"
    , timeout_env: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Invoke scripts/multimodal_fuel_emissions.py and return its final JSON dict.

    While running:
    - stream ONLY lines that look like logs (prefix '[') to our stdout
      and to our log file (if enabled);
    - suppress the final JSON from the console, but capture it to return.
    """
    if not script_path.exists():
        raise FileNotFoundError(f"multimodal_fuel_emissions.py not found at: {script_path}")

    cmd = [
          _sys.executable
        , str(script_path)
        , "--origin", origin
        , "--destiny", destiny
        , "--cargo-t", str(cargo_t)
    ]

    if truck_key:
        cmd += [ "--truck-key", truck_key ]
    if diesel_price_override is not None:
        cmd += [ "--diesel-price-override", str(diesel_price_override) ]
    if cabotage_fuel_type:
        cmd += [ "--cabotage-fuel-type", cabotage_fuel_type ]
    if ors_profile:
        cmd += [ "--ors-profile", ors_profile ]

    # Boolean flags (use the same semantics as the child)
    if not fallback_to_car:
        cmd += [ "--no-fallback-to-car" ]
    if overwrite_routes:
        cmd += [ "--overwrite" ]
    if not include_ops_and_hotel:
        cmd += [ "--no-include-ops-hotel" ]

    if routes_db_path is not None:
        cmd += [ "--db-path", str(routes_db_path) ]
    if routes_table is not None:
        cmd += [ "--table", routes_table ]
    if ports_json is not None:
        cmd += [ "--ports-json", str(ports_json) ]
    if sea_matrix_json is not None:
        cmd += [ "--sea-matrix-json", str(sea_matrix_json) ]
    if hotel_json is not None:
        cmd += [ "--hotel-json", str(hotel_json) ]

    _LOG.debug("Exec: %s", " ".join(cmd))

    # Child env: force UTF-8 and pass our log-level + timeout knobs
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env["CARBON_LOG_LEVEL"] = log_level  # picked up by init_logging() in the child
    if timeout_env:
        env.update(timeout_env)
        _LOG.debug(
              "Forwarding ORS timeouts to child: %s"
            , {k: v for k, v in timeout_env.items() if k.startswith("ORS_")}
        )

    proc = subprocess.Popen(
          cmd
        , stdout=subprocess.PIPE
        , stderr=subprocess.PIPE
        , text=True
        , encoding="utf-8"
        , errors="replace"
        , bufsize=1     # line-buffered
        , env=env
    )

    out_lines: List[str] = []
    err_lines: List[str] = []
    log_file_path = get_current_log_path()

    def _pump(pipe, collector: List[str]) -> None:
        try:
            for line in iter(pipe.readline, ""):
                if not line:
                    break
                collector.append(line)
                if _is_log_line(line):
                    _sys.stdout.write(line)
                    _append_to_file(log_file_path, line)
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    t_out = threading.Thread(target=_pump, args=(proc.stdout, out_lines), daemon=True)
    t_err = threading.Thread(target=_pump, args=(proc.stderr, err_lines), daemon=True)
    t_out.start()
    t_err.start()

    code = proc.wait()
    t_out.join()
    t_err.join()

    stdout = "".join(out_lines)
    stderr = "".join(err_lines)

    if code != 0:
        _LOG.warning(
              "multimodal_fuel_emissions failed for '%s' (code=%s)"
            , destiny
            , code
        )
        # Child logs already printed; surface a concise error
        raise RuntimeError(f"multimodal_fuel_emissions failed for '{destiny}' (code={code})")

    # Parse final JSON (prefer stdout; fallback to combined)
    try:
        return _extract_last_json_object(stdout)
    except Exception:
        return _extract_last_json_object(stdout + "\n" + stderr)


# ────────────────────────────────────────────────────────────────────────────────
# DB helpers for multimodal results
# ────────────────────────────────────────────────────────────────────────────────

def _safe_float(
    v: Any
) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _result_to_mm_kwargs(
    payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Transform the JSON payload from `multimodal_fuel_emissions.py` into the
    kwargs expected by `upsert_multimodal_result(...)`.

    Expected JSON shape (simplified):

        {
          "origin_label": "...",
          "destiny_label": "...",
          "cargo_t": 30.0,
          "scenarios": {
            "road_only": {
              "distance_km": ...,
              "fuel_liters": ...,
              "fuel_kg": ...,
              "fuel_cost_r": ...,
              "co2e_kg": ...
            },
            "multimodal": {
              "road": {...},
              "sea": {...},
              "totals": {...}
            }
          },
          ...
        }
    """
    origin_label = str(payload.get("origin_label") or payload.get("origin_raw") or "")
    destiny_label = str(payload.get("destiny_label") or payload.get("destiny_raw") or "")
    cargo_t = _safe_float(payload.get("cargo_t")) or 0.0

    scenarios = payload.get("scenarios") or {}
    road_only = scenarios.get("road_only") or {}
    mm = scenarios.get("multimodal") or {}
    mm_road = mm.get("road") or {}
    mm_sea = mm.get("sea") or {}
    mm_totals = mm.get("totals") or {}

    return {
          "origin_name": origin_label
        , "destiny_name": destiny_label
        , "cargo_t": float(cargo_t)

        , "road_only_distance_km": _safe_float(road_only.get("distance_km"))
        , "road_only_fuel_liters": _safe_float(road_only.get("fuel_liters"))
        , "road_only_fuel_kg": _safe_float(road_only.get("fuel_kg"))
        , "road_only_fuel_cost_r": _safe_float(road_only.get("fuel_cost_r"))
        , "road_only_co2e_kg": _safe_float(road_only.get("co2e_kg"))

        , "mm_road_fuel_liters": _safe_float(mm_road.get("fuel_liters"))
        , "mm_road_fuel_kg": _safe_float(mm_road.get("fuel_kg"))
        , "mm_road_fuel_cost_r": _safe_float(mm_road.get("fuel_cost_r"))
        , "mm_road_co2e_kg": _safe_float(mm_road.get("co2e_kg"))

        , "sea_distance_km": _safe_float(mm_sea.get("sea_km"))
        , "sea_fuel_kg": _safe_float(mm_sea.get("fuel_kg"))
        , "sea_fuel_cost_r": _safe_float(mm_sea.get("fuel_cost_r"))
        , "sea_co2e_kg": _safe_float(mm_sea.get("co2e_kg"))

        , "total_fuel_kg": _safe_float(mm_totals.get("fuel_kg"))
        , "total_fuel_cost_r": _safe_float(mm_totals.get("fuel_cost_r"))
        , "total_co2e_kg": _safe_float(mm_totals.get("co2e_kg"))

        , "delta_fuel_cost_r": _safe_float(mm_totals.get("delta_cost_vs_road_only_r"))
        , "delta_co2e_kg": _safe_float(mm_totals.get("delta_co2e_vs_road_only_kg"))
    }


def _load_existing_destinies(
    conn
    , table_name: str
) -> Set[str]:
    """
    For --resume: return set of destiny_name already stored in the multimodal table.
    If the table does not exist yet, returns an empty set.
    """
    try:
        ensure_multimodal_results_table(conn, table_name=table_name)
    except Exception:
        # if DDL fails, better to raise: but in a weird situation we just restart
        raise

    try:
        cur = conn.execute(
            f"SELECT destiny_name FROM {table_name};"
        )
        return {str(row[0]) for row in cur.fetchall() if row[0] is not None}
    except sqlite3.OperationalError:  # type: ignore[name-defined]
        # Table missing or other issue; safest is to return empty and let
        # ensure_multimodal_results_table create it properly.
        return set()


# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────

def main(
    argv: Optional[List[str]] = None
) -> int:
    ap = argparse.ArgumentParser(
        description="Bulk loader for multimodal fuel/emissions/costs into SQLite."
    )
    ap.add_argument(
          "--origin"
        , required=True
        , help="Origin (address/city/CEP/'lat,lon')."
    )
    ap.add_argument(
          "--cargo-t"
        , type=float
        , required=True
        , help="Cargo mass in tonnes."
    )
    ap.add_argument(
          "--dest-file"
        , type=Path
        , default=Path("data") / "city_dests.txt"
        , help="Text file with one destiny per line (default: data/city_dests.txt)."
    )

    # Multimodal results DB configuration
    ap.add_argument(
          "--db-path"
        , type=Path
        , default=DEFAULT_DB_PATH
        , help=f"SQLite path for both routes cache and multimodal results. Default: {DEFAULT_DB_PATH}"
    )
    ap.add_argument(
          "--mm-table"
        , default=None
        , help=(
            "Multimodal results table name. "
            "Default pattern: [OriginSanitized]__[AmountInTons]tons"
        )
    )

    # Pass-through knobs to the child (routes + fuel service)
    ap.add_argument(
          "--truck-key"
        , default="auto_by_weight"
        , help="Truck key for road legs (default: auto_by_weight)."
    )
    ap.add_argument(
          "--diesel-price-override"
        , type=float
        , default=None
        , help="Override diesel price [R$/L] for road legs (forwarded to child)."
    )
    ap.add_argument(
          "--cabotage-fuel-type"
        , choices=["vlsfo", "mfo"]
        , default="vlsfo"
        , help="Ship fuel type for sea leg (forwarded to child)."
    )
    ap.add_argument(
          "--ors-profile"
        , choices=["driving-hgv", "driving-car"]
        , default=None
        , help="Primary ORS profile (forwarded to child)."
    )

    ap.add_argument(
          "--no-fallback-to-car"
        , dest="fallback_to_car"
        , action="store_false"
        , help="Disable fallback to 'driving-car' in the child."
    )
    ap.set_defaults(fallback_to_car=True)

    ap.add_argument(
          "--no-include-ops-hotel"
        , dest="include_ops_and_hotel"
        , action="store_false"
        , help="Disable port ops + hotel fuel in cabotage leg (child)."
    )
    ap.set_defaults(include_ops_and_hotel=True)

    ap.add_argument(
          "--overwrite-routes"
        , action="store_true"
        , help="Ask the child to overwrite routing legs even if cached."
    )

    ap.add_argument(
          "--routes-table"
        , default="routes"
        , help="Routes table name for the ORS cache (forwarded to child)."
    )
    ap.add_argument(
          "--ports-json"
        , type=Path
        , default=None
        , help="Override ports JSON path (child)."
    )
    ap.add_argument(
          "--sea-matrix-json"
        , type=Path
        , default=None
        , help="Override sea matrix JSON path (child)."
    )
    ap.add_argument(
          "--hotel-json"
        , type=Path
        , default=None
        , help="Override hotel JSON path (child)."
    )

    # Repo logging knobs (use default logs folder when write_output=True)
    ap.add_argument(
          "--log-level"
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    ap.add_argument(
          "--write-output"
        , action="store_true"
        , help="Also write logs to file (default logs folder from your logging module)."
    )

    # Resume option (skip already stored destinies)
    ap.add_argument(
          "--resume"
        , action="store_true"
        , help="Skip destinies already present in the multimodal results table."
    )

    # ORS timeout / retry knobs (forwarded via env to the child)
    ap.add_argument(
          "--connect-timeout-s"
        , type=float
        , default=None
        , help="ORS connect timeout (s)."
    )
    ap.add_argument(
          "--read-timeout-fast-s"
        , type=float
        , default=None
        , help="ORS fast read timeout (s)."
    )
    ap.add_argument(
          "--read-timeout-slow-s"
        , type=float
        , default=None
        , help="ORS slow read timeout (s)."
    )
    ap.add_argument(
          "--max-retries"
        , type=int
        , default=None
        , help="Total HTTP retries for status codes."
    )
    ap.add_argument(
          "--backoff-s"
        , type=float
        , default=None
        , help="Backoff factor for HTTP retries."
    )
    ap.add_argument(
          "--no-escalate-on-timeout"
        , dest="escalate_on_timeout"
        , action="store_false"
        , help="Disable fast→slow salvage attempt on read timeout."
    )
    ap.set_defaults(escalate_on_timeout=True)

    args = ap.parse_args(argv)

    # initialise repo logger
    init_logging(
          level=args.log_level
        , force=True
        , write_output=args.write_output
    )
    _LOG.info(
          "Starting bulk multimodal fuel/emissions/costs | origin=%s | cargo=%.3f t"
        , args.origin
        , args.cargo_t
    )
    if args.write_output:
        _LOG.info("Log file → %s", get_current_log_path())

    # destination list
    dests = _read_dest_file(args.dest_file)
    _LOG.info("Destinations loaded: %d (from %s)", len(dests), args.dest_file)

    # multimodal table name
    origin_tag = _strip_accents_and_sanitize(args.origin)
    amount_tag = _amount_tag(args.cargo_t)
    mm_table_name = args.mm_table or f"{origin_tag}__{amount_tag}"
    _LOG.info("Multimodal results table → %s (db=%s)", mm_table_name, args.db_path)

    # build child timeout env dict
    timeout_env = _forward_timeout_env(
          connect_timeout_s=args.connect_timeout_s
        , read_timeout_fast_s=args.read_timeout_fast_s
        , read_timeout_slow_s=args.read_timeout_slow_s
        , max_retries=args.max_retries
        , backoff_s=args.backoff_s
        , escalate_on_timeout=args.escalate_on_timeout
    )
    if timeout_env:
        _LOG.info(
              "ORS timeouts/retries: %s"
            , ", ".join(f"{k}={v}" for k, v in timeout_env.items())
        )

    total = len(dests)
    processed = 0
    failed = 0
    started_at = time.time()

    try:
        with db_session(db_path=args.db_path) as conn:
            # Ensure table exists and (optionally) load existing destinies
            ensure_multimodal_results_table(conn, table_name=mm_table_name)

            already_done: Set[str] = set()
            if args.resume:
                # reuse the same connection inside the transaction
                cur = conn.execute(
                    f"SELECT destiny_name FROM {mm_table_name};"
                )
                already_done = {str(row[0]) for row in cur.fetchall() if row[0] is not None}
                if already_done:
                    _LOG.info(
                          "Resume active — skipping %d already stored destinies."
                        , len(already_done)
                    )

            for i, dest in enumerate(dests, start=1):
                if args.resume and dest in already_done:
                    _LOG.info("↷ [%d/%d] %s — skipped (resume)", i, total, dest)
                    continue

                _LOG.info("=" * 70)
                _LOG.info("→ [%d/%d] %s", i, total, dest)
                _LOG.info("=" * 70)

                t0 = time.time()
                try:
                    payload = _run_multimodal_fuel_emissions(
                          origin=args.origin
                        , destiny=dest
                        , cargo_t=args.cargo_t
                        , truck_key=args.truck_key
                        , diesel_price_override=args.diesel_price_override
                        , cabotage_fuel_type=args.cabotage_fuel_type
                        , ors_profile=args.ors_profile
                        , fallback_to_car=args.fallback_to_car
                        , include_ops_and_hotel=args.include_ops_and_hotel
                        , overwrite_routes=args.overwrite_routes
                        , routes_db_path=args.db_path
                        , routes_table=args.routes_table
                        , ports_json=args.ports_json
                        , sea_matrix_json=args.sea_matrix_json
                        , hotel_json=args.hotel_json
                        , log_level=args.log_level
                        , timeout_env=timeout_env
                    )

                    mm_kwargs = _result_to_mm_kwargs(payload)
                    upsert_multimodal_result(
                          conn
                        , table_name=mm_table_name
                        , **mm_kwargs
                    )

                    processed += 1
                    dt = time.time() - t0
                    _LOG.info(
                          "✓ stored in table '%s' in %.2fs — %s"
                        , mm_table_name
                        , dt
                        , dest
                    )

                except Exception as e:
                    failed += 1
                    dt = time.time() - t0
                    msg = str(e).splitlines()[0][:500]
                    _LOG.warning(
                          "Failed (not stored) '%s' after %.2fs due to error: %s"
                        , dest
                        , dt
                        , msg
                    )
                    continue

    except KeyboardInterrupt:
        elapsed = time.time() - started_at
        _LOG.error(
              "Interrupted by user after %.1fs. Partial DB kept at %s (table=%s, processed=%d, failed=%d)"
            , elapsed
            , args.db_path
            , mm_table_name
            , processed
            , failed
        )
        return 130  # typical SIGINT code

    elapsed = time.time() - started_at
    _LOG.info(
          "Done in %.1fs → db=%s table=%s (processed=%d, failed=%d, total=%d)"
        , elapsed
        , args.db_path
        , mm_table_name
        , processed
        , failed
        , total
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
