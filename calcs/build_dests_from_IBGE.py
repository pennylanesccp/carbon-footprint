#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build a dests.txt from an IBGE municipalities CSV, with lines formatted as:
    Cidade, UF

Designed for the CSV format like:
ConcatUF+Mun;IBGE;IBGE7;UF;Município;Região;População 2010;Porte;Capital;...
(where delimiter is ';' and encoding may be latin1).

Usage examples:
  # All Brazil (alphabetical by UF, then city)
  python calcs/build_dests_from_IBGE.py --csv data/Lista_Municipios_com_IBGE_Brasil_Versao_CSV.csv --out data/dests.txt

  # Only SP + RJ, keep only cities with population >= 50k, order by population desc and limit to top 200
  python calcs/build_dests_from_IBGE.py --csv data/ibge.csv --ufs SP,RJ --min-pop 50000 --order pop --limit 200 --out data/dests_sp_rj.txt

Notes:
- Normalizes curly quotes/apostrophes to ASCII (e.g., D´oeste → D'oeste), trims, collapses spaces.
- Skips empty/invalid rows; de-duplicates final "Cidade, UF" lines.
- Output is UTF-8 text, one destination per line.
"""

from __future__ import annotations

import argparse
import csv
import io
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Dict

# Columns we care about in the IBGE CSV (Portuguese, but we allow variants)
CITY_COLS = ["Município", "Municipio", "Municí­pio"]  # be tolerant to encoding glitches
UF_COL = "UF"
POP_COLS = ["População 2010", "Populacao 2010", "População", "Populacao"]

APOSTROPHE_ALIASES = ["´", "’", "‘", "`"]  # map to "'"

def _norm_city(name: str) -> str:
    if name is None:
        return ""
    s = str(name).strip()
    for bad in APOSTROPHE_ALIASES:
        s = s.replace(bad, "'")
    # collapse multiple spaces
    while "  " in s:
        s = s.replace("  ", " ")
    return s

def _coerce_int(x: str) -> int:
    try:
        return int(str(x).strip().replace(".", "").replace(",", ""))
    except Exception:
        return 0

def _pick_col(row: Dict[str, str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in row:
            return row[c]
    # Try case-insensitive fallback
    lower = {k.lower(): k for k in row.keys()}
    for c in candidates:
        k = lower.get(c.lower())
        if k:
            return row[k]
    return None

def read_ibge_rows(csv_path: Path) -> Iterable[Dict[str, str]]:
    # Try UTF-8 first, then latin1
    data = csv_path.read_bytes()
    text = None
    for enc in ("utf-8-sig", "latin1"):
        try:
            text = data.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        raise UnicodeError("Could not decode file as UTF-8 or latin1.")

    # Force semicolon delimiter; rely on csv module
    f = io.StringIO(text)
    reader = csv.DictReader(f, delimiter=";")
    for row in reader:
        yield row

def build_list(
    csv_path: Path,
    ufs: Optional[List[str]] = None,
    min_pop: int = 0,
    order: str = "alpha",  # "alpha" or "pop"
    limit: Optional[int] = None,
) -> List[Tuple[str, str, int]]:
    ufs = [u.strip().upper() for u in (ufs or []) if u.strip()] or None
    out: List[Tuple[str, str, int]] = []

    for row in read_ibge_rows(csv_path):
        uf = (row.get(UF_COL) or "").strip().upper()
        if not uf:
            continue
        if ufs and uf not in ufs:
            continue

        city = _pick_col(row, CITY_COLS)
        city = _norm_city(city or "")
        if not city:
            continue

        pop_raw = _pick_col(row, POP_COLS) or ""
        pop = _coerce_int(pop_raw)

        if pop < min_pop:
            continue

        out.append((city, uf, pop))

    # De-duplicate by (city, uf), keep max population if duplicates
    uniq: Dict[Tuple[str, str], int] = {}
    for city, uf, pop in out:
        key = (city, uf)
        if key not in uniq or pop > uniq[key]:
            uniq[key] = pop

    items = [(c, u, p) for (c, u), p in uniq.items()]

    if order == "pop":
        items.sort(key=lambda t: (t[2], t[1], t[0]), reverse=True)
    else:
        # default alpha: UF, City
        items.sort(key=lambda t: (t[1], t[0]))

    if limit:
        items = items[: int(limit)]
    return items

def write_dests(items: List[Tuple[str, str, int]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        for city, uf, _ in items:
            f.write(f"{city}, {uf}\n")

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Build dests.txt from IBGE municipalities CSV.")
    ap.add_argument("--csv", type=Path, required=True, help="Path to IBGE CSV (semicolon-delimited).")
    ap.add_argument("--ufs", type=str, default="", help="Comma-separated list of UFs to include (e.g., SP,RJ,MG). Empty = all.")
    ap.add_argument("--min-pop", type=int, default=0, help="Minimum population to include (default 0).")
    ap.add_argument("--order", choices=["alpha", "pop"], default="alpha", help="Sort by 'alpha' (UF, city) or 'pop' (desc).")
    ap.add_argument("--limit", type=int, default=0, help="Optional max number of rows to output.")
    ap.add_argument("--out", type=Path, default=Path("data") / "dests.txt", help="Output text file path.")
    args = ap.parse_args(argv)

    ufs = [u.strip() for u in args.ufs.split(",") if u.strip()] if args.ufs else None
    items = build_list(
        csv_path=args.csv,
        ufs=ufs,
        min_pop=int(args.min_pop),
        order=args.order,
        limit=(args.limit or None),
    )
    write_dests(items, args.out)
    print(f"Wrote {len(items)} lines → {args.out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
