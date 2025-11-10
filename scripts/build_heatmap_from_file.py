# scripts/build_heatmap_from_file.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build heatmap CSV by looping `scripts/single_evaluation.py` over each destiny in
`data/city_dests.txt` (default). The CSV contains **cabotage − road** deltas.

Filename pattern:
    [OriginSanitized]__[AmountInTons]tons.csv
Examples:
    Sao_Paulo__26tons.csv
    Av_Luciano_Gualberto__50tons.csv

Usage (PowerShell):
    # venv active; ORS_API_KEY set
    python scripts/build_heatmap_from_file.py `
      --origin "São Paulo, SP" `
      --amount-tons 26 `
      --log-level INFO `
      --write-output `
      --echo-csv
# Notes
# • Rows with failures are SKIPPED (not written to CSV); failures are only logged.
# • CSV columns: destiny, delta_fuel_cost_brl, delta_co2e_kg  (no error / no delta_fuel_kg)
# • When --write-output is set, the log file is written inside --outdir.
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
import csv
import json
import os
import re
import subprocess
import unicodedata
from typing import Any, Dict, List, Optional

# standardized repo logging
from modules.functions.logging import init_logging, get_logger, get_current_log_path
_LOG = get_logger(__name__)

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _strip_accents_and_sanitize(s: str) -> str:
    """Remove accents; keep letters/digits/space/_; collapse spaces to underscores."""
    n = unicodedata.normalize("NFKD", s)
    n = "".join(ch for ch in n if not unicodedata.combining(ch))
    n = re.sub(r"[^A-Za-z0-9 _]", " ", n)
    n = re.sub(r"\s+", "_", n.strip())
    return n

def _amount_tag(tons: float) -> str:
    """Format amount for filename (int → '26tons', else e.g. '26_5tons')."""
    if abs(tons - round(tons)) < 1e-9:
        return f"{int(round(tons))}tons"
    s = f"{tons:.2f}".rstrip("0").rstrip(".").replace(".", "_")
    return f"{s}tons"

def _read_dest_file(path: Path) -> List[str]:
    """Read destinations, ignoring blanks and lines starting with '#'. """
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

def _extract_last_json_object(text: str) -> Dict[str, Any]:
    """
    single_evaluation.py logs a lot and then prints one final JSON object.
    Grab the last balanced {...} block from the provided text and parse it.
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
    raise ValueError("Could not parse final JSON from single_evaluation output")

# ────────────────────────────────────────────────────────────────────────────────
# Child process runner (captured; no tee → no duplicate logs on parent console)
# ────────────────────────────────────────────────────────────────────────────────
def _run_single_evaluation(
      *
    , origin: str
    , destiny: str
    , amount_tons: float
    , truck: Optional[str] = "auto_by_weight"      # default
    , empty_backhaul: Optional[float] = None
    , ors_profile: Optional[str] = None
    , fallback_to_car: bool = True                 # default ON
    , diesel_prices_csv: Optional[Path] = None
    , script_path: Path = Path("scripts") / "single_evaluation.py"
) -> Dict[str, Any]:
    """Invoke scripts/single_evaluation.py and return its final JSON dict."""
    if not script_path.exists():
        raise FileNotFoundError(f"single_evaluation.py not found at: {script_path}")

    cmd = [
        _sys.executable, str(script_path),
        "--origin", origin,
        "--destiny", destiny,
        "--amount-tons", str(amount_tons),
    ]
    if truck:
        cmd += ["--truck", truck]
    if empty_backhaul is not None:
        cmd += ["--empty-backhaul", str(empty_backhaul)]
    if ors_profile:
        cmd += ["--ors-profile", ors_profile]
    if fallback_to_car:
        cmd += ["--fallback-to-car"]
    if diesel_prices_csv:
        cmd += ["--diesel-prices-csv", str(diesel_prices_csv)]

    _LOG.debug("Exec: %s", " ".join(cmd))

    # Force UTF-8 in the child so Unicode logs (→, Δ, accents) won't crash on Windows
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    if proc.returncode != 0:
        _LOG.warning("single_evaluation failed for '%s' (code=%s)", destiny, proc.returncode)
        _LOG.debug("STDOUT:\n%s", stdout)
        _LOG.debug("STDERR:\n%s", stderr)
        raise RuntimeError(f"single_evaluation failed for '{destiny}' (code={proc.returncode})")

    try:
        return _extract_last_json_object(stdout)
    except Exception:
        return _extract_last_json_object(stdout + "\n" + stderr)

# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Build heatmap CSV by looping single_evaluation over a destination list."
    )
    ap.add_argument("--origin", required=True, help="Origin (address/city/CEP/'lat,lon').")
    ap.add_argument("--amount-tons", type=float, required=True, help="Cargo mass in tonnes.")
    ap.add_argument(
        "--dest-file",
        type=Path,
        default=Path("data") / "city_dests.txt",
        help="Text file with one destination per line (default: data/city_dests.txt).",
    )
    ap.add_argument(
        "--outdir",
        type=Path,
        default=Path("outputs"),
        help="Directory to write the CSV and (optionally) the .log file.",
    )

    # pass-through to child (defaults as requested)
    ap.add_argument("--truck", default="auto_by_weight", help="Truck key (default: auto_by_weight).")
    ap.add_argument("--empty-backhaul", type=float, default=None, help="Empty backhaul share (0..1).")
    ap.add_argument(
        "--ors-profile",
        choices=["driving-hgv", "driving-car"],
        default=None,
        help="Primary ORS profile.",
    )
    ap.add_argument(
        "--no-fallback-to-car",
        dest="fallback_to_car",
        action="store_false",
        help="Disable fallback to 'driving-car'.",
    )
    ap.set_defaults(fallback_to_car=True)

    ap.add_argument(
        "--diesel-prices-csv",
        type=Path,
        default=None,
        help="Forward a custom diesel prices CSV to single_evaluation.py.",
    )

    # repo logging knobs
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    ap.add_argument(
        "--write-output",
        action="store_true",
        help="Also write logs to a file placed INSIDE --outdir.",
    )

    # echo each CSV row to the logger as soon as it's written
    ap.add_argument(
        "--echo-csv",
        action="store_true",
        help="Echo each CSV line to the logger as it's written (prefix 'CSV+').",
    )

    args = ap.parse_args(argv)

    # initialize repo logger (send file logs to OUTDIR if requested)
    init_logging(
        level=args.log_level,
        force=True,
        write_output=args.write_output,
        logs_dir=str(args.outdir),   # ← log file goes inside outputs/
    )
    _LOG.info("Starting heatmap build | origin=%s | amount=%.3f t", args.origin, args.amount_tons)
    if args.write_output:
        _LOG.info("Log file → %s", get_current_log_path())

    # load destination list
    dests = _read_dest_file(args.dest_file)
    _LOG.info("Destinations loaded: %d (from %s)", len(dests), args.dest_file)

    # output file name
    origin_tag = _strip_accents_and_sanitize(args.origin)
    amount_tag = _amount_tag(args.amount_tons)
    out_path = args.outdir / f"{origin_tag}__{amount_tag}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _LOG.info("Output CSV → %s", out_path)

    # CSV columns (NO error, NO delta_fuel_kg)
    cols = ["destiny", "delta_fuel_cost_brl", "delta_co2e_kg"]

    # ── STREAM rows as they are produced ────────────────────────────────────────
    total = len(dests)
    written = 0
    failed = 0
    try:
        with out_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            f.flush()  # header visible immediately

            for i, dest in enumerate(dests, start=1):
                _LOG.info("→ [%d/%d] %s", i, total, dest)
                try:
                    result = _run_single_evaluation(
                        origin=args.origin,
                        destiny=dest,
                        amount_tons=args.amount_tons,
                        truck=args.truck,
                        empty_backhaul=args.empty_backhaul,
                        ors_profile=args.ors_profile,
                        fallback_to_car=args.fallback_to_car,
                        diesel_prices_csv=args.diesel_prices_csv,
                    )
                    d = dict(result.get("deltas_cabotage_minus_road", {}))
                    row = {
                        "destiny": dest,
                        "delta_fuel_cost_brl": float(d.get("cost_brl", 0.0)),
                        "delta_co2e_kg": float(d.get("co2e_kg", 0.0)),
                    }

                    # write one line now, then flush so tailers can see it
                    w.writerow(row)
                    f.flush()
                    written += 1

                    if args.echo_csv:
                        _LOG.info("CSV+ %s", ",".join(str(row.get(c, "")) for c in cols))

                except Exception as e:
                    failed += 1
                    _LOG.warning("Filtered (not written) '%s' due to error: %s", dest, str(e).splitlines()[0][:500])
                    continue  # skip row

    except KeyboardInterrupt:
        _LOG.error("Interrupted by user. Partial CSV kept at %s (rows=%d, failed=%d)", out_path, written, failed)
        return 130  # typical SIGINT code

    _LOG.info("Done → %s (rows=%d, failed=%d, total=%d)", out_path, written, failed, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
