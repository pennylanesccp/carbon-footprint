import os
import requests
import pandas as pd
import logging

# --- Configurações ---

# URL oficial da ANP para a série histórica de preços estaduais
ANP_URL = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal/semanal-estados-desde-2013.xlsx"

# Diretório para salvar os dados (bom para organizar o projeto)
DATA_DIR = "_data"

# Caminho para o arquivo Excel baixado
EXCEL_FILE_PATH = os.path.join(DATA_DIR, "semanal-estados-desde-2013.xlsx")

# Caminho para o arquivo CSV final que queremos criar
OUTPUT_CSV_PATH = os.path.join(DATA_DIR, "latest_diesel_prices.csv")

# O nome exato da planilha dentro do arquivo Excel
# (Baseado no nome do arquivo de amostra)
SHEET_NAME = "ESTADOS - DESDE 30.12.2012"

# O produto que estamos interessados
TARGET_PRODUCT = "OLEO DIESEL S10"

# Número de linhas de cabeçalho a pular (baseado na amostra)
HEADER_ROWS_TO_SKIP = 17

# Mapeamento de Nomes de Estado (como estão no CSV) para UFs
# (Baseado nas amostras de "PARA", "PARAIBA", "PARANA" - tudo em maiúsculas)
STATE_TO_UF_MAP = {
    "ACRE": "AC",
    "ALAGOAS": "AL",
    "AMAPA": "AP",
    "AMAZONAS": "AM",
    "BAHIA": "BA",
    "CEARA": "CE",
    "DISTRITO FEDERAL": "DF",
    "ESPIRITO SANTO": "ES",
    "GOIAS": "GO",
    "MARANHAO": "MA",
    "MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS",
    "MINAS GERAIS": "MG",
    "PARA": "PA",
    "PARAIBA": "PB",
    "PARANA": "PR",
    "PERNAMBUCO": "PE",
    "PIAUI": "PI",
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS",
    "RONDONIA": "RO",
    "RORAIMA": "RR",
    "SANTA CATARINA": "SC",
    "SAO PAULO": "SP",
    "SERGIPE": "SE",
    "TOCANTINS": "TO"
}

# Configuração do logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def download_anp_file(url: str, save_path: str) -> bool:
    """
    Baixa o arquivo da ANP. Retorna True se for bem-sucedido, False caso contrário.
    """
    logging.info(f"Tentando baixar o arquivo de {url}...")
    try:
        response = requests.get(url, stream=True, timeout=30)
        
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Arquivo salvo com sucesso em {save_path}")
            return True
        else:
            logging.warning(
                f"Falha ao baixar o arquivo. Status Code: {response.status_code}"
            )
            return False
            
    except requests.RequestException as e:
        logging.error(f"Erro de rede ao tentar baixar o arquivo: {e}")
        return False

def process_anp_excel(excel_path: str, output_path: str):
    """
    Processa o arquivo Excel baixado e salva um CSV limpo com os preços mais recentes.
    """
    logging.info(f"Processando o arquivo Excel: {excel_path}...")
    try:
        df = pd.read_excel(
            excel_path, 
            sheet_name=SHEET_NAME, 
            header=HEADER_ROWS_TO_SKIP
        )
    except Exception as e:
        logging.error(f"Não foi possível ler o arquivo Excel. Erro: {e}")
        logging.error(f"Verifique se o nome da planilha '{SHEET_NAME}' e o 'header={HEADER_ROWS_TO_SKIP}' estão corretos.")
        return

    # 1. Renomear colunas para facilitar o uso
    df = df.rename(columns={
        'ESTADO': 'ESTADO',
        'PRODUTO': 'PRODUTO',
        'DATA FINAL': 'DATA FINAL',
        'PREÇO MÉDIO REVENDA': 'PRECO_MEDIO'
    })

    # 2. Filtrar colunas necessárias
    cols_to_keep = ['DATA FINAL', 'ESTADO', 'PRODUTO', 'PRECO_MEDIO']
    if not all(col in df.columns for col in cols_to_keep):
        logging.error("O arquivo Excel não contém as colunas esperadas. (Ex: 'ESTADO', 'PRODUTO', 'DATA FINAL', 'PREÇO MÉDIO REVENDA')")
        return
        
    df = df[cols_to_keep]

    # 3. Filtrar pelo produto de interesse
    df_diesel = df[df['PRODUTO'] == TARGET_PRODUCT].copy()
    if df_diesel.empty:
        logging.warning(f"Nenhum dado encontrado para o produto '{TARGET_PRODUCT}'")
        return

    # 4. Encontrar os dados mais recentes
    df_diesel['DATA FINAL'] = pd.to_datetime(df_diesel['DATA FINAL'])
    latest_date = df_diesel['DATA FINAL'].max()
    df_latest = df_diesel[df_diesel['DATA FINAL'] == latest_date]
    
    logging.info(f"Dados mais recentes encontrados para a data: {latest_date.date()}")

    # 5. Mapear Nomes de Estado para UF
    df_latest['UF'] = df_latest['ESTADO'].map(STATE_TO_UF_MAP)
    
    # Verificar se algum estado não foi mapeado
    unmapped = df_latest[df_latest['UF'].isnull()]['ESTADO'].unique()
    if unmapped:
        logging.warning(f"Estados não mapeados encontrados: {unmapped}. Eles serão ignorados.")
        
    df_latest = df_latest.dropna(subset=['UF']) # Remover linhas sem UF

    # 6. Criar o DataFrame final
    df_final = df_latest[['UF', 'PRECO_MEDIO']]
    df_final = df_final.rename(columns={'PRECO_MEDIO': 'price'})
    
    # 7. Salvar o arquivo CSV final
    df_final.to_csv(output_path, index=False, float_format='%.3f')
    logging.info(f"Arquivo de preços de diesel salvo com sucesso em: {output_path}")

def update_diesel_prices():
    """
    Função principal para orquestrar o download e processamento.
    """
    # Garantir que o diretório _data exista
    os.makedirs(DATA_DIR, exist_ok=True)
    
    if download_anp_file(ANP_URL, EXCEL_FILE_PATH):
        process_anp_excel(EXCEL_FILE_PATH, OUTPUT_CSV_PATH)
    else:
        logging.warning("O download falhou. O processamento será pulado.")
        logging.warning(f"Usando o arquivo CSV anterior em {OUTPUT_CSV_PATH}, se existir.")

# --- Bloco de Execução ---
if __name__ == "__main__":
    """
    Permite que o script seja executado diretamente do terminal
    usando: python diesel_price_updater.py
    """
    update_diesel_prices()