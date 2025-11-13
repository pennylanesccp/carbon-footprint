# calcs/hotel.py
# Build per-city hotel-at-berth factors from ANTAQ Atracação TXT (Cabotagem only).
# Style: commas at the beginning of the line; 4 spaces; verbose comments.

from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────

INPUT_TXT  = os.path.join("data", "cabotage_data", "2025Atracacao.txt")
OUTPUT_JSON = os.path.join("data", "cabotage_data", "hotel.json")

R_HOTEL_KG_PER_HOUR = 135.0         # auxiliary genset fuel rate (kg/h)
ASSUMED_T_PER_CALL  = 6000.0        # tonnes handled per call (conservative placeholder)
NAV_REQUIRED        = "Cabotagem"   # filter Tipo de Navegação da Atracação

# Required headers exactly as in the TXT (semicolon-delimited, PT-BR)
H = {
      "id": "IDAtracacao"
    , "cidade": "Município"
    , "uf": "UF"
    , "navegacao": "Tipo de Navegação da Atracação"
    , "atracacao": "Data Atracação"
    , "desatracacao": "Data Desatracação"
    , "inicio_op": "Data Início Operação"
    , "termino_op": "Data Término Operação"
}

DT_FORMAT = "%d/%m/%Y %H:%M:%S"

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, DT_FORMAT)
    except ValueError:
        return None

def _berth_hours(row: dict) -> Optional[float]:
    """
    Prefer Desatracação − Atracação; fallback to Término − Início Operação.
    Returns hours (float) or None if missing.
    """
    a = _parse_dt(row.get(H["atracacao"], ""))
    d = _parse_dt(row.get(H["desatracacao"], ""))
    if a and d:
        return (d - a).total_seconds() / 3600.0
    iop = _parse_dt(row.get(H["inicio_op"], ""))
    top = _parse_dt(row.get(H["termino_op"], ""))
    if iop and top:
        return (top - iop).total_seconds() / 3600.0
    return None

# ────────────────────────────────────────────────────────────────────────────────
# Main build
# ────────────────────────────────────────────────────────────────────────────────

def build_hotel_json(
    *
    , input_txt: str = INPUT_TXT
    , output_json: str = OUTPUT_JSON
    , r_hotel_kg_per_hour: float = R_HOTEL_KG_PER_HOUR
    , assumed_t_per_call: float = ASSUMED_T_PER_CALL
    , nav_required: str = NAV_REQUIRED
) -> dict:
    """
    Reads ANTAQ TXT and writes hotel.json with:
      unit='kg_fuel_per_tonne', scope='hotel_at_berth', entries=[{city, uf, calls, avg_berth_h, total_hotel_fuel_kg, total_handled_t, kg_fuel_per_t}, ...]
    Only rows where Tipo de Navegação da Atracação == 'Cabotagem' are considered.
    """
    if not os.path.exists(input_txt):
        raise FileNotFoundError(f"Input file not found: {input_txt}")

    # aggregate per city (and keep UF)
    sums_hours: Dict[str, float] = {}
    counts: Dict[str, int] = {}
    city_uf: Dict[str, str] = {}

    with open(input_txt, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f, delimiter=";")
        headers = rdr.fieldnames or []
        # quick header sanity
        missing = [v for v in H.values() if v not in headers]
        if missing:
            raise ValueError(f"❌ Missing required columns: {missing}")

        total_rows = 0
        kept_cabotagem = 0

        for row in rdr:
            total_rows += 1
            # filter by navigation type (Cabotagem only)
            nav = (row.get(H["navegacao"], "") or "").strip()
            if nav.lower() != nav_required.lower():
                continue  # drop non-Cabotagem calls

            kept_cabotagem += 1
            city = (row.get(H["cidade"], "") or "").strip()
            uf   = (row.get(H["uf"], "") or "").strip()
            if not city:
                # ignore rows without city label
                continue

            hrs = _berth_hours(row)
            if hrs is None or hrs < 0:
                # skip rows we cannot compute
                continue

            sums_hours[city] = sums_hours.get(city, 0.0) + hrs
            counts[city]     = counts.get(city, 0) + 1
            if city not in city_uf:
                city_uf[city] = uf

    # Build entries
    entries: List[dict] = []
    for city, calls in sorted(counts.items()):
        hrs_sum = sums_hours.get(city, 0.0)
        avg_h   = (hrs_sum / calls) if calls > 0 else 0.0
        total_fuel = hrs_sum * r_hotel_kg_per_hour
        total_t    = calls * assumed_t_per_call
        kg_per_t   = (total_fuel / total_t) if total_t > 0 else 0.0

        entries.append({
              "city": city
            , "uf": city_uf.get(city, "")
            , "calls": calls
            , "avg_berth_h": round(avg_h, 3)
            , "total_hotel_fuel_kg": round(total_fuel, 2)
            , "total_handled_t": round(total_t, 2)
            , "kg_fuel_per_t": round(kg_per_t, 6)
        })

    payload = {
          "unit": "kg_fuel_per_tonne"
        , "scope": "hotel_at_berth"
        , "R_hotel_kg_per_hour": r_hotel_kg_per_hour
        , "assumed_t_per_call": assumed_t_per_call
        , "note": "Cabotagem only; kg_fuel_per_t = sum(hotel_fuel_kg)/sum(handled_t) per city. Berth time from Atracação→Desatracação, fallback Início→Término."
        , "entries": entries
        , "header_debug": {
              "original_headers": list(headers)
            , "filter": {"Tipo de Navegação da Atracação": nav_required}
        }
    }

    # write JSON
    os.makedirs(os.path.dirname(output_json), exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # console summary (optional)
    print(f"✓ Read rows: {total_rows}")
    print(f"✓ Cabotagem kept: {kept_cabotagem}")
    print(f"✓ Cities: {len(entries)}")
    print(f"→ Wrote: {output_json}")

    return payload


if __name__ == "__main__":
    build_hotel_json()
