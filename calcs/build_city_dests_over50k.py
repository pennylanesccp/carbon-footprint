#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds 'city_dests_over50k.txt' from 'Lista_Municipios_com_IBGE_Brasil_Versao_CSV.csv'.

This script is a modified version with hardcoded parameters:
- Input: Lista_Municipios_com_IBGE_Brasil_Versao_CSV.csv
- Output: city_dests_over50k.txt
- Filter: Includes only cities with a population *greater than* 50,000.
- Format: "Cidade, UF", sorted alphabetically by UF, then City.

It expects the CSV to have a semicolon (;) delimiter and columns for
'Município', 'UF', and 'População 2010'.
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
            print(f"Successfully decoded file with {enc}.")
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

def main() -> int:
    # --- Hardcoded parameters based on the request ---
    INPUT_CSV = Path("data/raw/destinies/Lista_Municipios_com_IBGE_Brasil_Versao_CSV.csv")
    OUTPUT_FILE = Path("data/processed/destinies/city_dests_over50k.txt")
    
    # "more than 50000" means the minimum population is 50001
    # The build_list logic is `if pop < min_pop: continue`, so 50001 is correct.
    MIN_POPULATION = 50001 
    
    ORDER = "alpha" # Default sort order (UF, City)
    UFS_FILTER = None # All UFs
    LIMIT_ROWS = None # No limit
    # --- End of hardcoded parameters ---

    # Check if input file exists
    if not INPUT_CSV.exists():
        print(f"Error: Input file not found at '{INPUT_CSV}'")
        print("Please make sure the file is in the same directory as the script.")
        return 1

    print(f"Reading from: {INPUT_CSV}")
    print(f"Filtering for: Population > 50,000 (min_pop = {MIN_POPULATION})")
    
    items = build_list(
        csv_path=INPUT_CSV,
        ufs=UFS_FILTER,
        min_pop=MIN_POPULATION,
        order=ORDER,
        limit=LIMIT_ROWS,
    )
    
    write_dests(items, OUTPUT_FILE)
    
    print(f"\nSuccessfully wrote {len(items)} lines to -> {OUTPUT_FILE}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())