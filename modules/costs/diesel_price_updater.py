# modules/costs/diesel_price_updater.py
# -*- coding: utf-8 -*-

"""
Weekly diesel price updater — integrates with repo logging.

This module:
  • Checks if latest_diesel_prices.csv is older than 7 days.
  • If outdated, downloads ANP Excel, parses it, and writes a cleaned CSV.
  • If still fresh, skips download and processing.
"""

from __future__ import annotations

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
import pandas as pd

# ────────────────────────────────
# Repo paths and logger
# ────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from modules.infra.logging import get_logger   # our main logger

logger = get_logger(__name__)

# ────────────────────────────────
# Config
# ────────────────────────────────
ANP_URL = (
    "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/"
    "precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal/"
    "semanal-estados-desde-2013.xlsx"
)

RAW_DIR = ROOT / "data" / "raw" / "road_data"
PROCESSED_DIR = ROOT / "data" / "processed" / "road_data"

EXCEL_FILE_PATH = RAW_DIR / "semanal-estados-desde-2013.xlsx"
OUTPUT_CSV_PATH = PROCESSED_DIR / "latest_diesel_prices.csv"

SHEET_NAME = "ESTADOS - DESDE 30.12.2012"
TARGET_PRODUCT = "OLEO DIESEL S10"
HEADER_ROWS_TO_SKIP = 17

STATE_TO_UF_MAP = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPA": "AP", "AMAZONAS": "AM", "BAHIA": "BA",
    "CEARA": "CE", "DISTRITO FEDERAL": "DF", "ESPIRITO SANTO": "ES", "GOIAS": "GO",
    "MARANHAO": "MA", "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG",
    "PARA": "PA", "PARAIBA": "PB", "PARANA": "PR", "PERNAMBUCO": "PE", "PIAUI": "PI",
    "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN", "RIO GRANDE DO SUL": "RS",
    "RONDONIA": "RO", "RORAIMA": "RR", "SANTA CATARINA": "SC", "SAO PAULO": "SP",
    "SERGIPE": "SE", "TOCANTINS": "TO"
}

# ────────────────────────────────
# Core logic
# ────────────────────────────────

def file_is_fresh(path: Path, max_age_days: int = 7) -> bool:
    """Return True if the file exists and is newer than max_age_days."""
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=max_age_days)


def download_anp_file(url: str, save_path: Path) -> bool:
    logger.info(f"Downloading ANP diesel price Excel → {save_path}")
    try:
        res = requests.get(url, stream=True, timeout=30)
        if res.status_code != 200:
            logger.warning(f"ANP download failed, status={res.status_code}")
            return False

        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            for chunk in res.iter_content(8192):
                f.write(chunk)

        logger.info("Excel downloaded successfully.")
        return True

    except Exception as e:
        logger.error(f"Network error: {e}")
        return False


def process_anp_excel(excel_path: Path, output_path: Path) -> None:
    logger.info(f"Processing ANP Excel → {excel_path}")

    try:
        df = pd.read_excel(
            excel_path,
            sheet_name=SHEET_NAME,
            header=HEADER_ROWS_TO_SKIP
        )
    except Exception as e:
        logger.error(f"Failed to read Excel: {e}")
        return

    df = df.rename(columns={
        "ESTADO": "ESTADO",
        "PRODUTO": "PRODUTO",
        "DATA FINAL": "DATA_FINAL",
        "PREÇO MÉDIO REVENDA": "PRECO_MEDIO"
    })

    needed = ["DATA_FINAL", "ESTADO", "PRODUTO", "PRECO_MEDIO"]
    if not all(c in df.columns for c in needed):
        logger.error("Missing expected columns in ANP file.")
        return

    df = df[needed]
    df = df[df["PRODUTO"] == TARGET_PRODUCT].copy()

    df["DATA_FINAL"] = pd.to_datetime(df["DATA_FINAL"])
    latest = df["DATA_FINAL"].max()
    df_latest = df[df["DATA_FINAL"] == latest].copy()

    logger.info(f"Latest diesel price date: {latest.date()}")

    df_latest["UF"] = df_latest["ESTADO"].map(STATE_TO_UF_MAP)
    unmapped = df_latest[df_latest["UF"].isna()]["ESTADO"].unique()

    if len(unmapped) > 0:
        logger.warning(f"Unmapped states: {unmapped}")

    df_latest = df_latest.dropna(subset=["UF"])

    out = df_latest[["UF", "PRECO_MEDIO"]].rename(columns={"PRECO_MEDIO": "price"})
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False, float_format="%.3f")

    logger.info(f"Saved cleaned diesel prices → {output_path}")


def update_diesel_prices() -> None:
    """Main pipeline entrypoint."""
    if file_is_fresh(OUTPUT_CSV_PATH):
        logger.info("Diesel price file is fresh (<7 days) — skipping update.")
        return

    logger.info("Diesel price file is OLD — updating now.")
    if download_anp_file(ANP_URL, EXCEL_FILE_PATH):
        process_anp_excel(EXCEL_FILE_PATH, OUTPUT_CSV_PATH)
    else:
        logger.warning("ANP download failed; keeping existing CSV.")


# ────────────────────────────────
# Direct CLI execution
# ────────────────────────────────
if __name__ == "__main__":
    print("[diesel_price_updater] Running standalone test…")
    update_diesel_prices()
