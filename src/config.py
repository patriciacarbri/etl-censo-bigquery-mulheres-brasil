import os
from dotenv import load_dotenv
from pathlib import Path

# Carrega o .env da raiz
ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(ENV_PATH)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_2010 = os.getenv("BQ_DATASET_2010")
BQ_DATASET_2022 = os.getenv("BQ_DATASET_2022")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
TRUSTED_CSV_DIR = DATA_DIR / "trusted" / "csv"
TRUSTED_PARQUET_DIR = DATA_DIR / "trusted" / "parquet"
OUTPUT_DICT_DIR = DATA_DIR / "trusted"

for path in [RAW_DIR, TRUSTED_CSV_DIR, TRUSTED_PARQUET_DIR, OUTPUT_DICT_DIR]:
    path.mkdir(parents=True, exist_ok=True)
