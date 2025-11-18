# modules/costs/diesel_prices.py
# -*- coding: utf-8 -*-
"""
Diesel price helpers
====================

Main entry point
----------------
- get_average_price(
      origin=None
    , destiny=None
    , *
    , uf_o=None
    , uf_d=None
    , default_price_r_per_l=6.0
    , csv_path=None
  )

Usage patterns
--------------
1) Legacy / label-based (works with current road_fuel_service):

   get_average_price(origin="São Paulo, SP, Brazil",
                     destiny="Pacatuba, CE, Brazil")

   → UFs are *parsed* from the labels as a fallback.

2) UF-based (preferred going forward):

   get_average_price(uf_o="SP", uf_d="CE")

   → UFs are taken directly; labels may be omitted.

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


# ────────────────────────────── UF helper (optional) ───────────────────────────


def _extract_uf_from_label(label: str | None) -> Optional[str]:
    """
    Best-effort UF extractor from a label like:
        'Avenida Professor Luciano Gualberto, São Paulo, Brazil'
        'Pacatuba, CE, Brazil'
        'São Paulo, SP'

    Strategy (very simple on purpose):
      - split by comma;
      - scan tokens from right to left and return the first 2-letter alpha token.

    Returns
    -------
    Optional[str]
        UF code like 'SP', 'CE', 'RJ', or None if not found.
    """
    if not label:
        return None

    parts = [p.strip() for p in str(label).split(",") if p.strip()]
    for token in reversed(parts):
        if len(token) == 2 and token.isalpha():
            return token.upper()

    return None


# ───────────────────────── diesel prices table loader ──────────────────────────


def load_latest_diesel_price(
    csv_path: str | Path | None = None,
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
            "UF": df_raw[uf_col].astype(str).str.upper().str.strip(),
            "price_brl_l": pd.to_numeric(df_raw[price_col], errors="coerce"),
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
    uf_o: str,
    uf_d: str,
    table: pd.DataFrame,
    *,
    source_csv: str | Path | None = None,
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
      origin: str | None = None
    , destiny: str | None = None
    , *
    , uf_o: str | None = None
    , uf_d: str | None = None
    , default_price_r_per_l: float = 6.0
    , csv_path: str | Path | None = None
) -> Dict[str, Any]:
    """
    Adapter used by modules.fuel.road_fuel_service._resolve_diesel_price.

    Signature is intentionally compatible with:

        get_average_price(origin=..., destiny=...)

    but also accepts pre-resolved UF codes:

        get_average_price(uf_o="SP", uf_d="CE")

    Parameters
    ----------
    origin : str | None
        Full origin label (e.g. 'Avenida Professor Luciano Gualberto, São Paulo, Brazil').
    destiny : str | None
        Full destiny label (e.g. 'Pacatuba, CE, Brazil').
    uf_o : str | None
        Origin UF code (e.g. 'SP'). If provided, takes precedence over parsing `origin`.
    uf_d : str | None
        Destiny UF code (e.g. 'CE'). If provided, takes precedence over parsing `destiny`.
    default_price_r_per_l : float, optional
        Fallback price when we cannot extract UFs or table is missing/empty.
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
          "fallback_used": false,
          "origin_label": "...",
          "destiny_label": "..."
        }

    Notes
    -----
    - road_fuel_service will treat this as a metadata payload and extract
      'price_r_per_l', passing the remaining keys into `RoadFuelProfile.extra`.
    """
    origin_label = (origin or "").strip()
    destiny_label = (destiny or "").strip()

    table = load_latest_diesel_price(csv_path=csv_path)

    # Decide UFs: explicit uf_o/uf_d from caller win; else we try to parse from labels.
    uf_o_final: Optional[str] = uf_o or _extract_uf_from_label(origin_label)
    uf_d_final: Optional[str] = uf_d or _extract_uf_from_label(destiny_label)

    # Fallback if we have no usable table or UFs
    if table.empty or not uf_o_final or not uf_d_final:
        price = float(default_price_r_per_l)
        reason_parts = []
        if table.empty:
            reason_parts.append("empty_or_missing_table")
        if not uf_o_final:
            reason_parts.append("missing_uf_origin")
        if not uf_d_final:
            reason_parts.append("missing_uf_destiny")
        reason = ",".join(reason_parts) or "unknown"

        _log.warning(
            "get_average_price: falling back to default price. "
            "origin=%r destiny=%r uf_o=%r uf_d=%r default=%.4f reason=%s",
            origin_label,
            destiny_label,
            uf_o_final,
            uf_d_final,
            price,
            reason,
        )
        return {
              "price_r_per_l": price
            , "source": "default_price_param"
            , "uf_origin": uf_o_final
            , "uf_destiny": uf_d_final
            , "fallback_used": True
            , "csv_path": str(csv_path or DEFAULT_DIESEL_PRICES_CSV)
            , "origin_label": origin_label or None
            , "destiny_label": destiny_label or None
            , "fallback_reason": reason
        }

    # Normal path: table loaded and both UFs known
    avg_price, ctx = avg_price_for_ufs(
          uf_o_final
        , uf_d_final
        , table
        , source_csv=csv_path or DEFAULT_DIESEL_PRICES_CSV
    )

    meta: Dict[str, Any] = dict(ctx)
    meta["price_r_per_l"] = float(avg_price)
    meta["source"] = "latest_diesel_prices_csv"
    meta["origin_label"] = origin_label or None
    meta["destiny_label"] = destiny_label or None

    _log.debug(
        "get_average_price: origin=%r destiny=%r uf_o=%s uf_d=%s → avg_price=%.4f R$/L",
        origin_label,
        destiny_label,
        uf_o_final,
        uf_d_final,
        avg_price,
    )
    return meta


# ─────────────────────────────── CLI smoke test ────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    """
    Small CLI / smoke test for diesel price helpers.

    Examples
    --------
    # Using labels (will try to parse UFs from ', SP' / ', CE' etc.)
    python -m modules.costs.diesel_prices \
        --origin "São Paulo, SP, Brazil" \
        --destiny "Rio de Janeiro, RJ, Brazil"

    # Using explicit UF codes (ignores label parsing)
    python -m modules.costs.diesel_prices \
        --uf-o SP \
        --uf-d CE
    """
    import argparse
    import json

    from modules.infra.logging import init_logging

    parser = argparse.ArgumentParser(
        description=(
            "Diesel price helpers smoke test: load CSV and compute "
            "average price between two UFs or address labels."
        )
    )
    parser.add_argument(
          "--origin"
        , default="São Paulo, SP, Brazil"
        , help="Origin label (optional if uf-o is provided)."
    )
    parser.add_argument(
          "--destiny"
        , default="Rio de Janeiro, RJ, Brazil"
        , help="Destiny label (optional if uf-d is provided)."
    )
    parser.add_argument(
          "--uf-o"
        , default=None
        , help="Origin UF code (e.g. SP). If provided, overrides parsing from --origin."
    )
    parser.add_argument(
          "--uf-d"
        , default=None
        , help="Destiny UF code (e.g. CE). If provided, overrides parsing from --destiny."
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
          origin=args.origin
        , destiny=args.destiny
        , uf_o=args.uf_o
        , uf_d=args.uf_d
        , default_price_r_per_l=args.default_price
        , csv_path=args.csv_path
    )

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
