# modules/costs/diesel_prices.py
# -*- coding: utf-8 -*-
"""
Diesel price helpers (UF-based)
===============================

Main entry point
----------------
- get_average_price(
      uf_o
    , uf_d
    , default_price_r_per_l=6.0
    , csv_path=None
  )

Internal helpers
----------------
- load_latest_diesel_price(csv_path) -> pandas.DataFrame with columns ['UF', 'price_brl_l']
- avg_price_for_ufs(uf_o, uf_d, table, source_csv=...) -> (avg_price, context_dict)

CSV expectations
----------------
A header with at least:
  - 'UF'           (state code, e.g. 'SP', 'RJ', 'CE')
  - 'price_brl_l'  (float, diesel price in BRL/L)

Column names are case-insensitive; common aliases are auto-normalized.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from modules.infra.logging import get_logger

_log = get_logger(__name__)

# Repo-relative default CSV path: <repo_root>/data/processed/road_data/latest_diesel_prices.csv
# modules/costs/diesel_prices.py → parents[0]=costs, [1]=modules, [2]=repo_root
_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DIESEL_PRICES_CSV: Path = (
      _REPO_ROOT
    / "data"
    / "processed"
    / "road_data"
    / "latest_diesel_prices.csv"
)


# ───────────────────────── diesel prices table loader ──────────────────────────


def load_latest_diesel_price(
      csv_path: str | Path | None = None
) -> pd.DataFrame:
    """
    Load diesel price table.

    Parameters
    ----------
    csv_path : str | Path | None
        Explicit path. If None, defaults to:
            <repo_root>/data/processed/road_data/latest_diesel_prices.csv

    Returns
    -------
    pd.DataFrame
        Columns: ['UF', 'price_brl_l'] (both normalized).
        If file missing, returns an empty DataFrame.
    """
    path: Path = Path(csv_path) if csv_path is not None else DEFAULT_DIESEL_PRICES_CSV

    if not path.is_file():
        _log.warning(
            "load_latest_diesel_price: CSV not found at '%s'. Returning empty DataFrame.",
            path,
        )
        return pd.DataFrame(columns=["UF", "price_brl_l"])

    df_raw = pd.read_csv(path)

    # Case-insensitive column resolution
    cols_map = {c.lower().strip(): c for c in df_raw.columns}
    uf_col = cols_map.get("uf") or cols_map.get("state") or "UF"
    price_col = (
          cols_map.get("price_brl_l")
        or cols_map.get("price")
        or cols_map.get("diesel_price_brl_l")
        or cols_map.get("price_brl")
        or "price_brl_l"
    )

    df = pd.DataFrame(
        {
              "UF": df_raw[uf_col].astype(str).str.upper().str.strip()
            , "price_brl_l": pd.to_numeric(df_raw[price_col], errors="coerce")
        }
    ).dropna(subset=["price_brl_l"])

    _log.info(
        "load_latest_diesel_price: loaded %d rows from '%s'.",
        len(df),
        path,
    )
    return df


# ───────────────────────────── UF → avg price helper ───────────────────────────


def avg_price_for_ufs(
      uf_o: str
    , uf_d: str
    , table: pd.DataFrame
    , *
    , source_csv: str | Path | None = None
) -> Tuple[float, Dict[str, Any]]:
    """
    Average diesel price for origin/destiny UFs with fallbacks.

    Parameters
    ----------
    uf_o : str
        Origin UF code.
    uf_d : str
        Destiny UF code.
    table : pd.DataFrame
        Diesel price table with columns ['UF','price_brl_l'].
    source_csv : str | Path | None
        Path used to load the table (for logging / metadata only).

    Fallback logic
    --------------
    - If only one UF exists in the table → use that one for both.
    - If neither exists or table is empty → avg set to 0.0 with fallback_used=True.

    Returns
    -------
    (avg_price, context_dict)
      context_dict includes:
        uf_origin, uf_destiny, price_origin, price_destiny,
        source_csv, fallback_used (bool)
    """
    source_csv_str = str(source_csv) if source_csv is not None else str(DEFAULT_DIESEL_PRICES_CSV)

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

    ctx: Dict[str, Any] = {
          "uf_origin": uf_o or None
        , "uf_destiny": uf_d or None
        , "price_origin": None if p_o is None else float(p_o)
        , "price_destiny": None if p_d is None else float(p_d)
        , "source_csv": source_csv_str
        , "fallback_used": bool(fallback_used)
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


# ───────────────────────────── public helper for service ───────────────────────


def get_average_price(
      uf_o: str
    , uf_d: str
    , *
    , default_price_r_per_l: float = 6.0
    , csv_path: str | Path | None = None
) -> Dict[str, Any]:
    """
    UF-based adapter used by modules.fuel.road_fuel_service._resolve_diesel_price.

    Parameters
    ----------
    uf_o : str
        Origin UF code, e.g. 'SP'.
    uf_d : str
        Destiny UF code, e.g. 'CE'.
    default_price_r_per_l : float, optional
        Fallback price when we cannot find UFs in the table or table is missing/empty.
    csv_path : str | Path | None, optional
        Optional override path for the diesel prices CSV.

    Returns
    -------
    Dict[str, Any]
        A dict compatible with road_fuel_service expectations, e.g.:

        {
          "price_r_per_l": 5.9234,
          "source": "latest_diesel_prices_csv",
          "uf_origin": "SP",
          "uf_destiny": "CE",
          "price_origin": 5.80,
          "price_destiny": 6.05,
          "source_csv": ".../latest_diesel_prices.csv",
          "fallback_used": false
        }
    """
    uf_o_norm = (uf_o or "").upper().strip()
    uf_d_norm = (uf_d or "").upper().strip()

    table = load_latest_diesel_price(csv_path=csv_path)

    # Fallback if we have no usable table or UFs
    if table.empty or not uf_o_norm or not uf_d_norm:
        price = float(default_price_r_per_l)
        reason_parts: list[str] = []
        if table.empty:
            reason_parts.append("empty_or_missing_table")
        if not uf_o_norm:
            reason_parts.append("missing_uf_origin")
        if not uf_d_norm:
            reason_parts.append("missing_uf_destiny")
        reason = ",".join(reason_parts) or "unknown"

        _log.warning(
            "get_average_price: falling back to default price. "
            "uf_o=%r uf_d=%r default=%.4f reason=%s",
            uf_o_norm,
            uf_d_norm,
            price,
            reason,
        )
        return {
              "price_r_per_l": price
            , "source": "default_price_param"
            , "uf_origin": uf_o_norm or None
            , "uf_destiny": uf_d_norm or None
            , "fallback_used": True
            , "csv_path": str(csv_path or DEFAULT_DIESEL_PRICES_CSV)
            , "fallback_reason": reason
        }

    # Normal path: table loaded and both UFs known
    avg_price, ctx = avg_price_for_ufs(
          uf_o_norm
        , uf_d_norm
        , table
        , source_csv=csv_path or DEFAULT_DIESEL_PRICES_CSV
    )

    meta: Dict[str, Any] = dict(ctx)
    meta["price_r_per_l"] = float(avg_price)
    meta["source"] = "latest_diesel_prices_csv"

    _log.debug(
        "get_average_price: uf_o=%s uf_d=%s → avg_price=%.4f R$/L",
        uf_o_norm,
        uf_d_norm,
        avg_price,
    )
    return meta


# ─────────────────────────────── CLI smoke test ────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    """
    Small CLI / smoke test for diesel price helpers.

    Examples
    --------
    python -m modules.costs.diesel_prices --uf-origin SP --uf-destiny RJ
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
        , dest="uf_origin"
        , default="SP"
        , help="Origin UF code (default: SP)."
    )
    parser.add_argument(
          "--uf-destiny"
        , dest="uf_destiny"
        , default="RJ"
        , help="Destiny UF code (default: RJ)."
    )
    parser.add_argument(
          "--csv-path"
        , default=str(DEFAULT_DIESEL_PRICES_CSV)
        , help=f"Diesel prices CSV path (default: {DEFAULT_DIESEL_PRICES_CSV})."
    )
    parser.add_argument(
          "--default-price"
        , type=float
        , default=6.0
        , help="Default fallback price (R$/L) if UF/table not available."
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

    payload = get_average_price(
          uf_o=args.uf_origin
        , uf_d=args.uf_destiny
        , default_price_r_per_l=args.default_price
        , csv_path=args.csv_path
    )

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
