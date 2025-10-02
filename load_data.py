import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

df = pd.read_csv("data/ventes_clean.csv")
df.columns = [c.strip() for c in df.columns]
# Convertir date en datetime
df["date_vente"] = pd.to_datetime(df["date_vente"], format="%Y-%m-%d")

engine = create_engine(f"postgresql://postgres:{os.getenv("PWD_POSTGRES")}@localhost:5432/dbt_ventes")

df.to_sql("stg_ventes", engine, schema="raw_cleaned", if_exists="replace", index=False)

print("✅ Table stg_ventes chargée dans PostgreSQL avec succès.")