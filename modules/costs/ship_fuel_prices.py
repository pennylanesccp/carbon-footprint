# modules/costs/ship_fuel_prices.py
# -*- coding: utf-8 -*-
"""
Ship fuel prices (Santos, Brazil) from Ship & Bunker
====================================================

Scrapes https://shipandbunker.com/prices/br-brazil and extracts the
bunker prices for the port of **Santos**.

HTML pattern (simplified)
-------------------------
<tr ...>
  <th ...><a ...>Santos</a></th>
  <td class="price up ">
      <span title="480.00" class="quote tSearch">480.00</span>
      ...
  </td>
  <td class="price up ">
      <span title="812.50" class="quote tSearch">812.50</span>
      ...
  </td>
  <td class="price noprice "><span ...>-</span></td>
  <td class="date ">Nov 14</td>
</tr>

We take:
  • first price <td> → VLSFO (USD/mt)
  • second price <td> → MGO  (USD/mt)

This module can also:
  • convert those prices to BRL/mt using the `CurrencyConverter` package
  • append a simple text line with BRL prices to a local file
"""

from __future__ import annotations

import os
import re
from datetime import date
from typing import Dict, Any, Optional, List

import requests
from currency_converter import CurrencyConverter

from modules.infra.logging import get_logger

_log = get_logger(__name__)

SHIPANDBUNKER_BR_URL = "https://shipandbunker.com/prices/br-brazil"
SANTOS_LABEL = "Santos"

DEFAULT_OUTPUT_TXT = os.path.join(
      "data"
    , "processed"
    , "maritime_fuel"
    , "santos_bunker_brl.txt"
)

__all__ = [
      "SHIPANDBUNKER_BR_URL"
    , "fetch_santos_prices"
    , "apply_fx_brl"
    , "write_prices_txt"
]


# ────────────────────────────────────────────────────────────────────────────────
# HTML parsing helpers (regex-based, no external deps)
# ────────────────────────────────────────────────────────────────────────────────

_SANTOS_TR_RE = re.compile(
      r"<tr[^>]*>\s*"
      r"<th[^>]*>\s*<a[^>]*>\s*Santos\s*</a>\s*</th>"
      r"(?P<body>.*?)"
      r"</tr>"
    , flags=re.IGNORECASE | re.DOTALL
)

_PRICE_CELL_RE = re.compile(
      r'<td[^>]*class="price[^"]*"[^>]*>.*?'
      r'<span[^>]*title="(?P<price>\d+(?:\.\d+)?)"[^>]*>'
    , flags=re.IGNORECASE | re.DOTALL
)

_DATE_CELL_RE = re.compile(
      r'<td[^>]*class="date[^"]*"[^>]*>\s*([^<]+)\s*</td>'
    , flags=re.IGNORECASE | re.DOTALL
)


def _extract_santos_row(html: str) -> str | None:
    """
    Return the full <tr>...</tr> HTML for the Santos row, or None if not found.
    """
    m = _SANTOS_TR_RE.search(html)
    if not m:
        return None
    return m.group(0)


def _parse_prices_from_row(row_html: str) -> Dict[str, Any]:
    """
    Given the Santos <tr> HTML, extract VLSFO and MGO prices (USD/mt)
    and the date label.
    """
    prices = [
        float(m.group("price"))
        for m in _PRICE_CELL_RE.finditer(row_html)
    ]

    if len(prices) < 2:
        raise ValueError(
            f"Expected at least two price cells for Santos, found {len(prices)}. "
            "Row HTML snippet: "
            f"{row_html[:300]!r}..."
        )

    vlsfo_price = prices[0]
    mgo_price = prices[1]

    date_match = _DATE_CELL_RE.search(row_html)
    date_label = date_match.group(1).strip() if date_match else None

    _log.debug(
        "Parsed Santos row → VLSFO=%.2f USD/mt, MGO=%.2f USD/mt, date=%s",
        vlsfo_price,
        mgo_price,
        date_label,
    )

    return {
          "port": SANTOS_LABEL
        , "vlsfo_usd_per_mt": vlsfo_price
        , "mgo_usd_per_mt": mgo_price
        , "date_label": date_label
        , "source_url": SHIPANDBUNKER_BR_URL
        , "row_html_preview": row_html[:200]
    }


# ────────────────────────────────────────────────────────────────────────────────
# Public scraper (USD)
# ────────────────────────────────────────────────────────────────────────────────

def fetch_santos_prices(
    *,
    session: Optional[requests.Session] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """
    Fetch current Santos bunker prices (VLSFO and MGO) from Ship & Bunker.

    Parameters
    ----------
    session : Optional[requests.Session]
        Optional requests session for reuse / testing.
    timeout : float
        HTTP timeout in seconds (default: 30.0).

    Returns
    -------
    Dict[str, Any]
        {
          "port": "Santos",
          "vlsfo_usd_per_mt": float,
          "mgo_usd_per_mt": float,
          "date_label": str | None,
          "source_url": str,
          "row_html_preview": str,
        }

    Raises
    ------
    RuntimeError
        If the page cannot be fetched or parsed.
    """
    sess = session or requests.Session()
    headers = {
          "User-Agent": "carbon-footprint-tf1/1.0 (academic, non-commercial)"
    }

    _log.info("Fetching Santos bunker prices from %s", SHIPANDBUNKER_BR_URL)

    try:
        resp = sess.get(SHIPANDBUNKER_BR_URL, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except Exception as e:
        _log.error("HTTP error when fetching Ship & Bunker page: %s", e)
        raise RuntimeError("Failed to fetch Ship & Bunker Brazil prices page.") from e

    html = resp.text
    row_html = _extract_santos_row(html)

    if not row_html:
        _log.error(
            "Could not locate Santos <tr> row in Ship & Bunker HTML. "
            "First 400 chars of page: %r",
            html[:400],
        )
        raise RuntimeError("Failed to parse Santos row from Ship & Bunker page.")

    prices = _parse_prices_from_row(row_html)

    _log.info(
        "Santos bunker prices (USD): VLSFO=%.2f USD/mt, MGO=%.2f USD/mt (date=%s)",
        prices["vlsfo_usd_per_mt"],
        prices["mgo_usd_per_mt"],
        prices["date_label"],
    )
    return prices


# ────────────────────────────────────────────────────────────────────────────────
# FX conversion helpers (USD → BRL)
# ────────────────────────────────────────────────────────────────────────────────

def apply_fx_brl(
    prices: Dict[str, Any],
    *,
    converter: Optional[CurrencyConverter] = None,
) -> Dict[str, Any]:
    """
    Take a prices dict in USD from `fetch_santos_prices` and enrich it with
    BRL/mt values using the `CurrencyConverter` package.

    Returns a *new* dict with extra keys:

      - fx_brl_per_usd
      - vlsfo_brl_per_mt
      - mgo_brl_per_mt
      - run_date_iso
    """
    if "vlsfo_usd_per_mt" not in prices or "mgo_usd_per_mt" not in prices:
        raise ValueError(
            "apply_fx_brl expects keys 'vlsfo_usd_per_mt' and 'mgo_usd_per_mt' "
            f"but received keys={list(prices.keys())}"
        )

    c = converter or CurrencyConverter()

    # FX for 1 USD → BRL (ECB reference rate)
    fx_brl_per_usd = float(c.convert(1.0, "USD", "BRL"))
    _log.info(
        "FX rate used for conversion: 1 USD = %.6f BRL",
        fx_brl_per_usd,
    )

    vlsfo_usd = float(prices["vlsfo_usd_per_mt"])
    mgo_usd = float(prices["mgo_usd_per_mt"])

    vlsfo_brl = vlsfo_usd * fx_brl_per_usd
    mgo_brl = mgo_usd * fx_brl_per_usd

    _log.info(
        "Converted prices to BRL: "
        "VLSFO=%.2f USD/mt → %.2f BRL/mt; "
        "MGO=%.2f USD/mt → %.2f BRL/mt",
        vlsfo_usd,
        vlsfo_brl,
        mgo_usd,
        mgo_brl,
    )

    run_date_iso = date.today().isoformat()

    enriched = {
          **prices
        , "fx_brl_per_usd": fx_brl_per_usd
        , "vlsfo_brl_per_mt": vlsfo_brl
        , "mgo_brl_per_mt": mgo_brl
        , "run_date_iso": run_date_iso
    }

    _log.debug(
        "apply_fx_brl: enriched prices payload keys=%s",
        list(enriched.keys()),
    )
    return enriched


# ────────────────────────────────────────────────────────────────────────────────
# Simple TXT writer (BRL prices)
# ────────────────────────────────────────────────────────────────────────────────

def write_prices_txt(
    prices_brl: Dict[str, Any],
    *,
    output_path: str = DEFAULT_OUTPUT_TXT,
    append: bool = True,
) -> str:
    """
    Append (or overwrite) a simple text line with BRL prices.

    Line format (tab-separated):
        run_date_iso  date_label  vlsfo_brl_per_mt  mgo_brl_per_mt  fx_brl_per_usd

    Parameters
    ----------
    prices_brl : Dict[str, Any]
        Dict produced by `apply_fx_brl`.
    output_path : str
        Where to write the TXT file.
    append : bool
        If True (default), append line; else overwrite file.

    Returns
    -------
    str
        The absolute path of the written file.
    """
    required_keys = [
          "run_date_iso"
        , "date_label"
        , "vlsfo_brl_per_mt"
        , "mgo_brl_per_mt"
        , "fx_brl_per_usd"
    ]
    missing = [k for k in required_keys if k not in prices_brl]
    if missing:
        raise ValueError(
            f"write_prices_txt missing keys {missing} in prices_brl payload."
        )

    run_date_iso = str(prices_brl["run_date_iso"])
    date_label = str(prices_brl.get("date_label") or "")
    vlsfo_brl = float(prices_brl["vlsfo_brl_per_mt"])
    mgo_brl = float(prices_brl["mgo_brl_per_mt"])
    fx_brl_per_usd = float(prices_brl["fx_brl_per_usd"])

    # Ensure directory exists
    abs_path = os.path.abspath(output_path)
    out_dir = os.path.dirname(abs_path)
    os.makedirs(out_dir, exist_ok=True)

    mode = "a" if append else "w"
    line = (
          f"{run_date_iso}\t"
          f"{date_label}\t"
          f"{vlsfo_brl:.2f}\t"
          f"{mgo_brl:.2f}\t"
          f"{fx_brl_per_usd:.6f}\n"
    )

    _log.info(
        "Writing BRL prices to TXT file: path=%s, mode=%s, line=%r",
        abs_path,
        mode,
        line.strip(),
    )

    with open(abs_path, mode, encoding="utf-8") as f:
        f.write(line)

    return abs_path


# ────────────────────────────────────────────────────────────────────────────────
# CLI / smoke test
# ────────────────────────────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    """
    CLI smoke test.

    Examples
    --------
    python -m modules.costs.ship_fuel_prices
    python -m modules.costs.ship_fuel_prices --log-level DEBUG
    python -m modules.costs.ship_fuel_prices --output-txt data/processed/maritime_fuel/santos_bunker_brl.txt
    """
    import argparse
    import json

    from modules.infra.logging import init_logging

    parser = argparse.ArgumentParser(
        description=(
            "Fetch Santos bunker prices (VLSFO & MGO) from Ship & Bunker, "
            "convert to BRL/mt and write a simple TXT snapshot."
        )
    )
    parser.add_argument(
          "--log-level"
        , default="INFO"
        , choices=["DEBUG", "INFO", "WARNING", "ERROR"]
        , help="Logging level."
    )
    parser.add_argument(
          "--timeout"
        , type=float
        , default=30.0
        , help="HTTP timeout in seconds (default: 30.0)."
    )
    parser.add_argument(
          "--output-txt"
        , default=DEFAULT_OUTPUT_TXT
        , help=(
            "Path to TXT file where BRL prices will be appended. "
            f"Default: {DEFAULT_OUTPUT_TXT}"
        )
    )
    parser.add_argument(
          "--no-write"
        , action="store_true"
        , help="If set, do not write the TXT file (just print JSON)."
    )

    args = parser.parse_args(argv)

    init_logging(
          level=args.log_level
        , force=True
        , write_output=False
    )

    # 1) Fetch USD prices
    prices_usd = fetch_santos_prices(timeout=args.timeout)

    # 2) Convert to BRL
    prices_brl = apply_fx_brl(prices_usd)

    # 3) Optionally write TXT snapshot
    if not args.no_write:
        path = write_prices_txt(
              prices_brl
            , output_path=args.output_txt
            , append=True
        )
        _log.info("TXT snapshot written to %s", path)
        prices_brl["output_txt_path"] = path

    # 4) Print JSON payload (for CLI usage / debugging)
    print(json.dumps(prices_brl, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
