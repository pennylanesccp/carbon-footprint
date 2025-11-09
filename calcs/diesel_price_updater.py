import os
import requests
import pandas as pd
import logging
import sys

# --- Configuration ---
ANP_URL = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal/semanal-estados-desde-2013.xlsx"

try:
    # __file__ is the path to this script (calcs/diesel_price_updater.py)
    # Its parent is 'calcs', and the grandparent is the 'carbon-footprint' (REPO_ROOT)
    REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    # Fallback if run interactively (like in a notebook)
    REPO_ROOT = os.path.abspath(os.path.join(os.getcwd()))
    if not os.path.isdir(os.path.join(REPO_ROOT, "modules")):
        logging.error("Could not find repo root. Please run from the 'calcs' directory.")
        sys.exit(1)

# Set the correct output data directory for the road module
DATA_DIR = os.path.join(REPO_ROOT, "modules", "road", "_data")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)
    logging.info(f"Created data directory: {DATA_DIR}")

# Path to the large downloaded Excel file
EXCEL_FILE_PATH = os.path.join(DATA_DIR, "semanal-estados-desde-2013.xlsx")
# Path for the clean, final CSV
OUTPUT_CSV_PATH = os.path.join(DATA_DIR, "latest_diesel_prices.csv")

# Excel file specifics
SHEET_NAME = "ESTADOS - DESDE 30.12.2012"
TARGET_PRODUCT = "OLEO DIESEL S10"
HEADER_ROWS_TO_SKIP = 17

# State name to UF mapping
STATE_TO_UF_MAP = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPA": "AP", "AMAZONAS": "AM", "BAHIA": "BA",
    "CEARA": "CE", "DISTRITO FEDERAL": "DF", "ESPIRITO SANTO": "ES", "GOIAS": "GO",
    "MARANHAO": "MA", "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG",
    "PARA": "PA", "PARAIBA": "PB", "PARANA": "PR", "PERNAMBUCO": "PE", "PIAUI": "PI",
    "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN", "RIO GRANDE DO SUL": "RS",
    "RONDONIA": "RO", "RORAIMA": "RR", "SANTA CATARINA": "SC", "SAO PAULO": "SP",
    "SERGIPE": "SE", "TOCANTINS": "TO"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_anp_file(url: str, save_path: str) -> bool:
    """Downloads the ANP file. Returns True on success, False on failure."""
    logging.info(f"Attempting to download file from {url}...")
    try:
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"File saved successfully to {save_path}")
            return True
        else:
            logging.warning(f"Failed to download file. Status Code: {response.status_code}")
            return False
    except requests.RequestException as e:
        logging.error(f"Network error while trying to download file: {e}")
        return False

def process_anp_excel(excel_path: str, output_path: str):
    """Processes the downloaded Excel file and saves a clean CSV."""
    logging.info(f"Processing Excel file: {excel_path}...")
    try:
        df = pd.read_excel(
            excel_path, 
            sheet_name=SHEET_NAME, 
            header=HEADER_ROWS_TO_SKIP
        )
    except Exception as e:
        logging.error(f"Could not read the Excel file. Error: {e}")
        logging.error(f"Please check if sheet name '{SHEET_NAME}' and header skip '{HEADER_ROWS_TO_SKIP}' are correct.")
        return

    df = df.rename(columns={
        'ESTADO': 'ESTADO',
        'PRODUTO': 'PRODUTO',
        'DATA FINAL': 'DATA FINAL',
        'PREÇO MÉDIO REVENDA': 'PRECO_MEDIO'
    })

    cols_to_keep = ['DATA FINAL', 'ESTADO', 'PRODUTO', 'PRECO_MEDIO']
    if not all(col in df.columns for col in cols_to_keep):
        logging.error("Excel file does not contain expected columns (e.g., 'ESTADO', 'PRODUTO', 'DATA FINAL', 'PREÇO MÉDIO REVENDA')")
        return
        
    df = df[cols_to_keep]
    df_diesel = df[df['PRODUTO'] == TARGET_PRODUCT].copy()
    if df_diesel.empty:
        logging.warning(f"No data found for product '{TARGET_PRODUCT}'")
        return

    df_diesel['DATA FINAL'] = pd.to_datetime(df_diesel['DATA FINAL'])
    latest_date = df_diesel['DATA FINAL'].max()
    
    # --- FIX 1: Add .copy() to prevent SettingWithCopyWarning ---
    df_latest = df_diesel[df_diesel['DATA FINAL'] == latest_date].copy()
    
    logging.info(f"Found latest data for date: {latest_date.date()}")

    # --- This is where your warning came from ---
    df_latest['UF'] = df_latest['ESTADO'].map(STATE_TO_UF_MAP)
    
    unmapped = df_latest[df_latest['UF'].isnull()]['ESTADO'].unique()
    
    # --- FIX 2: Change `if unmapped:` to `if unmapped.size > 0:` ---
    if unmapped.size > 0:
        logging.warning(f"Found unmapped states: {unmapped}. They will be skipped.")
        
    df_latest = df_latest.dropna(subset=['UF']) # Remove unmapped rows

    df_final = df_latest[['UF', 'PRECO_MEDIO']]
    df_final = df_final.rename(columns={'PRECO_MEDIO': 'price'})
    
    df_final.to_csv(output_path, index=False, float_format='%.3f')
    logging.info(f"Diesel price file successfully saved to: {OUTPUT_CSV_PATH}")

def main():
    """Main function to orchestrate download and processing."""
    global OUTPUT_CSV_PATH  # Make it accessible in the main function
    
    if download_anp_file(ANP_URL, EXCEL_FILE_PATH):
        process_anp_excel(EXCEL_FILE_PATH, OUTPUT_CSV_PATH)
    else:
        logging.warning("Download failed. Processing will be skipped.")
        logging.warning(f"Using previous CSV file at {OUTPUT_CSV_PATH}, if it exists.")

if __name__ == "__main__":
    main()