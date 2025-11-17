# modules/costs/diesel_prices.py
# -*- coding: utf-8 -*-
"""
Diesel price helpers
====================

- load_latest_diesel_price(csv_path) → pandas.DataFrame with columns ['UF','price_brl_l']
- avg_price_for_ufs(uf_o, uf_d, table) → (avg_price, context_dict)

CSV expectations
----------------
A header with at least: 'UF' (state code), 'price_brl_l' (float).
Column names are case-insensitive; common aliases are auto-normalized.
"""

from __future__ import annotations

import os
from typing import Tuple, Dict, Any

import pandas as pd

from modules.infra.logging import get_logger

_log = get_logger(__name__)

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DIESEL_PRICES_CSV = os.path.join(
      "data"
    , "processed"
    , "road_data"
    , "latest_diesel_prices.csv"
)


def load_latest_diesel_price(csv_path: str | None = None) -> pd.DataFrame:
    """
    Load diesel price table.

    Parameters
    ----------
    csv_path : Optional[str]
        Explicit path. If None, defaults to data/processed/road_data/latest_diesel_prices.csv

    Returns
    -------
    pd.DataFrame
        Columns: ['UF','price_brl_l']
    """
    path = csv_path or DEFAULT_DIESEL_PRICES_CSV
    if not os.path.exists(path):
        _log.warning(
            "load_latest_diesel_price: CSV not found at '%s'. Returning empty DataFrame.",
            path,
        )
        return pd.DataFrame(columns=["UF", "price_brl_l"])

    df_raw = pd.read_csv(path)
    cols_map = {c.lower().strip(): c for c in df_raw.columns}
    uf_col = cols_map.get("uf") or cols_map.get("state") or "UF"
    price_col = (
        cols_map.get("price_brl_l")
        or cols_map.get("price")
        or cols_map.get("diesel_price_brl_l")
        or cols_map.get("price_brl")
        or "price_brl_l"
    )

    # Normalize
    df = pd.DataFrame(
        {
            "UF": df_raw[uf_col].astype(str).str.upper().str.strip(),
            "price_brl_l": pd.to_numeric(df_raw[price_col], errors="coerce"),
        }
    ).dropna(subset=["price_brl_l"])

    _log.info("load_latest_diesel_price: loaded %d rows from '%s'.", len(df), path)
    return df


def avg_price_for_ufs(
    uf_o: str,
    uf_d: str,
    table: pd.DataFrame,
    *,
    source_csv: str | None = None,
) -> Tuple[float, Dict[str, Any]]:
    """
    Average diesel price for origin/destiny UFs with fallbacks.

    Fallback logic
    --------------
    - If only one UF exists → use that one for both.
    - If neither exists or table empty → avg set to 0.0 with fallback_used=True.

    Returns
    -------
    (avg_price, context_dict)
      context_dict includes:
        uf_origin, uf_destiny, price_origin, price_destiny,
        source_csv, fallback_used (bool)
    """
    source_csv = source_csv or DEFAULT_DIESEL_PRICES_CSV
    uf_o = (uf_o or "").upper().strip()
    uf_d = (uf_d or "").upper().strip()

    def _lookup(uf: str) -> float | None:
        if not uf or table.empty:
            return None
        m = table.loc[table["UF"] == uf, "price_brl_l"]
        return None if m.empty else float(m.iloc[0])

    p_o = _lookup(uf_o)
    p_d = _lookup(uf_d)

    fallback_used = False
    if p_o is None and p_d is not None:
        p_o, fallback_used = p_d, True
    if p_d is None and p_o is not None:
        p_d, fallback_used = p_o, True

    if p_o is None and p_d is None:
        avg = 0.0
        fallback_used = True
    else:
        avg = (float(p_o) + float(p_d)) / 2.0

    ctx = {
        "uf_origin": uf_o or None,
        "uf_destiny": uf_d or None,
        "price_origin": None if p_o is None else float(p_o),
        "price_destiny": None if p_d is None else float(p_d),
        "source_csv": source_csv,
        "fallback_used": bool(fallback_used),
    }

    _log.info(
        "avg_price_for_ufs: uf_o=%s, uf_d=%s → price_o=%s, price_d=%s, avg=%.4f, fallback_used=%s",
        uf_o,
        uf_d,
        ctx["price_origin"],
        ctx["price_destiny"],
        avg,
        fallback_used,
    )
    return float(avg), ctx


# ────────────────────────────────────────────────────────────────────────────────
# CLI smoke test
# ────────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    """
    Small CLI / smoke test for diesel price helpers.

    Examples
    --------
    python -m modules.fuel.diesel_prices
    python -m modules.fuel.diesel_prices --uf-origin SP --uf-destiny RJ
    """
    import argparse
    import json
    from modules.infra.logging import init_logging

    parser = argparse.ArgumentParser(
        description=(
            "Diesel price helpers smoke test: load CSV and compute "
            "average price between two UFs."
        )
    )
    parser.add_argument(
          "--uf-origin"
        , default="SP"
        , help="Origin UF code (default: SP)."
    )
    parser.add_argument(
          "--uf-destiny"
        , default="RJ"
        , help="Destiny UF code (default: RJ)."
    )
    parser.add_argument(
          "--csv-path"
        , default=DEFAULT_DIESEL_PRICES_CSV
        , help=f"Diesel prices CSV path (default: {DEFAULT_DIESEL_PRICES_CSV})."
    )
    parser.add_argument(
          "--log-level"
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
        , help="Logging level for this smoke test."
    )

    args = parser.parse_args(argv)

    init_logging(
          level=args.log_level
        , force=True
        , write_output=False
    )

    table = load_latest_diesel_price(csv_path=args.csv_path)
    avg, ctx = avg_price_for_ufs(
          args.uf_origin
        , args.uf_destiny
        , table
        , source_csv=args.csv_path
    )

    payload = {
          "avg_price_brl_l": avg
        , "context": ctx
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
