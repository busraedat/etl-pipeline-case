import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT")
DATASET = os.getenv("BQ_DATASET_RAW", "raw")

ROOT = Path(__file__).resolve().parents[1]
SEARCH_DIRS = [ROOT / "assets", ROOT]

FILES = [
    ("users.csv", "customers"),
    ("addresses.csv", "addresses"),
    ("cities.csv", "cities"),
    ("states.csv", "states"),
    ("countries.csv", "countries"),
    ("neighborhoods.csv", "neighborhoods"),
    ("subscriptions.csv", "subscriptions"),
    ("orders.csv", "orders"),
    ("shipments.csv", "shipments"),
    ("Marketing Spend (TRY).csv", "MarketingSpend"),
]

def find_file(name: str) -> Path | None:
    for d in SEARCH_DIRS:
        p = d / name
        if p.exists():
            return p
    return None

def load_df(csv_path: Path) -> pd.DataFrame:
    seps = [",", ";", "\t", "|"]
    encodings = ["utf-8", "utf-8-sig", "latin1"]
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(csv_path, sep=sep, dtype=str, low_memory=False, encoding=enc)
                if df.shape[1] == 1 and sep != seps[-1]:
                    continue
                df.columns = [c.strip() for c in df.columns]
                df["ingest_date"] = pd.to_datetime(datetime.now().date())
                return df
            except Exception:
                continue
    df = pd.read_csv(csv_path, sep=None, engine="python", dtype=str, low_memory=False, on_bad_lines="skip", encoding="utf-8")
    df.columns = [c.strip() for c in df.columns]
    df["ingest_date"] = pd.to_datetime(datetime.now().date())
    return df

def upload_df_to_bq(df: pd.DataFrame, table_name: str) -> None:
    if bigquery is None or PROJECT is None:
        print(f"💡 BQ yok → {table_name}: {len(df)} satır | kolonlar: {list(df.columns)}")
        return
    client = bigquery.Client(project=PROJECT)
    table_id = f"{PROJECT}.{DATASET}.{table_name}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"✅ {table_id} tablosuna {len(df)} satır yüklendi.")

def main() -> None:
    any_found = False
    for file_name, table in FILES:
        path = find_file(file_name)
        if not path:
            print(f"⚠️ Bulunamadı: {file_name}")
            continue
        any_found = True
        print(f"⏳ Yükleniyor: {path.name}  →  {DATASET}.{table}")
        df = load_df(path)
        upload_df_to_bq(df, table)
    if not any_found:
        print("⚠️ Hiçbir kaynak CSV bulunamadı. Dosyaları 'assets/' veya proje köküne koymalısın.")

if __name__ == "__main__":
    main()
