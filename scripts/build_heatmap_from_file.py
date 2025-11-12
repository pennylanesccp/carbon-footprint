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
      --echo-csv `
      --connect-timeout-s 5 `
      --read-timeout-fast-s 8 `
      --read-timeout-slow-s 30 `
      --max-retries 1 `
      --backoff-s 0.2 `
      --resume

Notes
• Rows with failures are SKIPPED (not written to CSV); failures are only logged.
• CSV columns: destiny, delta_fuel_cost_brl, delta_co2e_kg  (no error / no delta_fuel_kg)
• When --write-output is set, the log file is written to the DEFAULT logs folder
  defined by `modules.functions.logging` (not inside --outdir).
• Timeout knobs are forwarded to the child via env so `ORSConfig`/`ORSClient` can pick them up:
    ORS_CONNECT_TIMEOUT_S, ORS_READ_TIMEOUT_FAST_S, ORS_READ_TIMEOUT_SLOW_S,
    ORS_MAX_RETRIES, ORS_BACKOFF_S, ORS_ESCALATE_ON_TIMEOUT.
• Eye-catching banners highlight each destination block for easier tailing.
• Optional --resume skips destinations already present in the output CSV.
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
import threading
import time
import unicodedata
from typing import Any, Dict, List, Optional, Set

# standardized repo logging
from modules.functions._logging import init_logging, get_logger, get_current_log_path
_LOG = get_logger("scripts.build_heatmap_from_file")  # explicit name for nicer prefixes

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
_BANNER = "=" * 70

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

def _is_log_line(s: str) -> bool:
    """
    Our log lines start with '[' (e.g., "[2025-11-10 09:42:50][INFO][modules…] …").
    The final JSON starts with '{'. Use that to decide what to show.
    """
    return s.lstrip().startswith("[")

def _append_to_file(path: Optional[str], text: str) -> None:
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
# Child process runner — stream ONLY log lines to console/file; keep JSON private
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
    , log_level: str = "INFO"                      # forward to child
    , script_path: Path = Path("scripts") / "single_evaluation.py"
    , timeout_env: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Invoke scripts/single_evaluation.py and return its final JSON dict.

    While running:
    - stream ONLY lines that look like logs (prefix '[') to our stdout
      and to our log file (if enabled);
    - suppress the final JSON from the console, but capture it to return.
    """
    if not script_path.exists():
        raise FileNotFoundError(f"single_evaluation.py not found at: {script_path}")

    cmd = [
          _sys.executable, str(script_path)
        , "--origin", origin
        , "--destiny", destiny
        , "--amount-tons", str(amount_tons)
    ]
    if truck:
        cmd += [ "--truck", truck ]
    if empty_backhaul is not None:
        cmd += [ "--empty-backhaul", str(empty_backhaul) ]
    if ors_profile:
        cmd += [ "--ors-profile", ors_profile ]
    if fallback_to_car:
        cmd += [ "--fallback-to-car" ]
    if diesel_prices_csv:
        cmd += [ "--diesel-prices-csv", str(diesel_prices_csv) ]

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

    # Capture buffers (for JSON) and stream logs to console/file
    out_lines: List[str] = []
    err_lines: List[str] = []
    log_file_path = get_current_log_path()

    def _pump(pipe, collector: List[str]):
        try:
            for line in iter(pipe.readline, ""):
                if not line:
                    break
                collector.append(line)
                if _is_log_line(line):
                    # forward child log exactly as-is (no re-logging → no double prefixes)
                    _sys.stdout.write(line)
                    _append_to_file(log_file_path, line)
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    t_out = threading.Thread(target=_pump, args=(proc.stdout, out_lines), daemon=True)
    t_err = threading.Thread(target=_pump, args=(proc.stderr, err_lines), daemon=True)
    t_out.start(); t_err.start()

    code = proc.wait()
    t_out.join(); t_err.join()

    stdout = "".join(out_lines)
    stderr = "".join(err_lines)

    if code != 0:
        # Log a single concise line here; child’s detailed logs already streamed.
        _LOG.warning("single_evaluation failed for '%s' (code=%s)", destiny, code)
        raise RuntimeError(f"single_evaluation failed for '{destiny}' (code={code})")

    # Parse the final JSON (prefer stdout; fallback to combined)
    try:
        return _extract_last_json_object(stdout)
    except Exception:
        return _extract_last_json_object(stdout + "\n" + stderr)

def _load_already_done(out_path: Path) -> Set[str]:
    """When --resume, read existing CSV and return the set of already processed 'destiny'."""
    if not out_path.exists():
        return set()
    try:
        done: Set[str] = set()
        with out_path.open("r", encoding="utf-8", newline="") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                d = (row.get("destiny") or "").strip()
                if d:
                    done.add(d)
        return done
    except Exception:
        _LOG.warning("Could not read existing CSV to resume; proceeding fresh.")
        return set()

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
          "--dest-file"
        , type=Path
        , default=Path("data") / "city_dests.txt"
        , help="Text file with one destination per line (default: data/city_dests.txt)."
    )
    ap.add_argument(
          "--outdir"
        , type=Path
        , default=Path("outputs")
        , help="Directory to write the CSV file(s)."
    )

    # pass-through to child (defaults as requested)
    ap.add_argument("--truck", default="auto_by_weight", help="Truck key (default: auto_by_weight).")
    ap.add_argument("--empty-backhaul", type=float, default=None, help="Empty backhaul share (0..1).")
    ap.add_argument(
          "--ors-profile"
        , choices=["driving-hgv", "driving-car"]
        , default=None
        , help="Primary ORS profile."
    )
    ap.add_argument(
          "--no-fallback-to-car"
        , dest="fallback_to_car"
        , action="store_false"
        , help="Disable fallback to 'driving-car'."
    )
    ap.set_defaults(fallback_to_car=True)

    ap.add_argument(
          "--diesel-prices-csv"
        , type=Path
        , default=None
        , help="Forward a custom diesel prices CSV to single_evaluation.py."
    )

    # repo logging knobs (now using DEFAULT logs folder when write_output=True)
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    ap.add_argument(
          "--write-output"
        , action="store_true"
        , help="Also write logs to file (default logs folder from your logging module)."
    )

    # echo each CSV row to the logger as soon as it's written
    ap.add_argument(
          "--echo-csv"
        , action="store_true"
        , help="Echo each CSV line to the logger as it's written (prefix 'CSV+')."
    )

    # resume option (skip already processed destinations)
    ap.add_argument(
          "--resume"
        , action="store_true"
        , help="If output CSV already exists, skip destinations already present."
    )

    # ── ORS timeout / retry knobs (forwarded via env to the child) ─────────────
    ap.add_argument("--connect-timeout-s", type=float, default=None, help="ORS connect timeout (s).")
    ap.add_argument("--read-timeout-fast-s", type=float, default=None, help="ORS fast read timeout (s).")
    ap.add_argument("--read-timeout-slow-s", type=float, default=None, help="ORS slow read timeout (s).")
    ap.add_argument("--max-retries", type=int, default=None, help="Total HTTP retries for status codes.")
    ap.add_argument("--backoff-s", type=float, default=None, help="Backoff factor for HTTP retries.")
    ap.add_argument(
          "--no-escalate-on-timeout"
        , dest="escalate_on_timeout"
        , action="store_false"
        , help="Disable fast→slow salvage attempt on read timeout."
    )
    ap.set_defaults(escalate_on_timeout=True)

    args = ap.parse_args(argv)

    # initialize repo logger — IMPORTANT: no logs_dir here → use default logs folder
    init_logging(
          level=args.log_level
        , force=True
        , write_output=args.write_output
    )
    _LOG.info("Starting heatmap build | origin=%s | amount=%.3f t", args.origin, args.amount_tons)
    if args.write_output:
        _LOG.info("Log file → %s", get_current_log_path())

    # load destination list
    dests = _read_dest_file(args.dest_file)
    _LOG.info("Destinations loaded: %d (from %s)", len(dests), args.dest_file)

    # output file name (CSV only)
    origin_tag = _strip_accents_and_sanitize(args.origin)
    amount_tag = _amount_tag(args.amount_tons)
    out_path = args.outdir / f"{origin_tag}__{amount_tag}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _LOG.info("Output CSV → %s", out_path)

    # CSV columns (NO error, NO delta_fuel_kg)
    cols = ["destiny", "delta_fuel_cost_brl", "delta_co2e_kg"]

    # resume bookkeeping
    already_done: Set[str] = _load_already_done(out_path) if args.resume else set()
    if already_done:
        _LOG.info("Resume active — skipping %d already processed destinations.", len(already_done))

    # build child timeout env dict (only keys with provided values)
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

    # ── STREAM rows as they are produced ────────────────────────────────────────
    total = len(dests)
    written = 0
    failed = 0
    started_at = time.time()
    try:
        # open file in append mode if resume and file exists; otherwise write header
        file_mode = "a" if (args.resume and out_path.exists()) else "w"
        with out_path.open(file_mode, encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            if file_mode == "w":
                w.writeheader()
                f.flush()  # header visible immediately

            for i, dest in enumerate(dests, start=1):
                if dest in already_done:
                    _LOG.info("↷ [%d/%d] %s — skipped (resume)", i, total, dest)
                    continue

                # Eye-catching banner
                _LOG.info(_BANNER)
                _LOG.info("→ [%d/%d] %s", i, total, dest)
                _LOG.info(_BANNER)

                t0 = time.time()
                try:
                    result = _run_single_evaluation(
                          origin=args.origin
                        , destiny=dest
                        , amount_tons=args.amount_tons
                        , truck=args.truck
                        , empty_backhaul=args.empty_backhaul
                        , ors_profile=args.ors_profile
                        , fallback_to_car=args.fallback_to_car
                        , diesel_prices_csv=args.diesel_prices_csv
                        , log_level=args.log_level
                        , timeout_env=timeout_env
                    )
                    d = dict(result.get("deltas_cabotage_minus_road", {}))
                    row = {
                          "destiny": dest
                        , "delta_fuel_cost_brl": float(d.get("cost_brl", 0.0))
                        , "delta_co2e_kg": float(d.get("co2e_kg", 0.0))
                    }

                    # write one line now, then flush so tailers can see it
                    w.writerow(row)
                    f.flush()
                    written += 1

                    if args.echo_csv:
                        _LOG.info("CSV+ %s", ",".join(str(row.get(c, "")) for c in cols))

                    dt = time.time() - t0
                    _LOG.info("✓ done in %.2fs — %s", dt, dest)

                except Exception as e:
                    failed += 1
                    dt = time.time() - t0
                    _LOG.warning(
                          "Filtered (not written) '%s' after %.2fs due to error: %s"
                        , dest
                        , dt
                        , str(e).splitlines()[0][:500]
                    )
                    continue  # skip row

    except KeyboardInterrupt:
        elapsed = time.time() - started_at
        _LOG.error(
              "Interrupted by user after %.1fs. Partial CSV kept at %s (rows=%d, failed=%d)"
            , elapsed
            , out_path
            , written
            , failed
        )
        return 130  # typical SIGINT code

    elapsed = time.time() - started_at
    _LOG.info(
          "Done in %.1fs → %s (rows=%d, failed=%d, total=%d)"
        , elapsed
        , out_path
        , written
        , failed
        , total
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
