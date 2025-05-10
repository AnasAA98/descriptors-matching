"""
db_ingest.py
------------
Load the two Excel sheets into MySQL

  • “Merchant List” ➜ merchant_list   (reference data)
  • “Descriptors”   ➜ transactions    (raw transactions)

Prereq: db_setup.py has already created the tables.
"""

import pandas as pd
from sqlalchemy import create_engine, types, text          
import config

def build_engine():
    url = (
        f"mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASS}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )
    return create_engine(url, echo=False, future=True)


def load_sheet(sheet_name: str) -> pd.DataFrame:
    """Read one sheet from the workbook into a DataFrame."""
    return pd.read_excel(
        "Descriptor_matching_assignment_for_ intership_applications.xlsx",
        sheet_name=sheet_name,
        engine="openpyxl",
    )


# ETL Step

def main() -> None:
    engine = build_engine()

    merchants_df = load_sheet("Merchant List")

    merchants_df.columns = merchants_df.columns.str.strip().str.lower().str.replace(" ", "_")

    print(f"{len(merchants_df):,} rows read from 'Merchant List' sheet")

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM merchant_list"))

    merchants_df.to_sql(
        name="merchant_list",
        con=engine,
        if_exists="append",
        index=False,
        dtype={
            "merchant_id":   types.String(36),
            "merchant_name": types.String(255),
            "merchant_city": types.String(100),
            "merchant_state": types.String(100),
            "merchant_postal": types.String(20),
            "merchant_country": types.String(100),
        },
    )
    print("✅ merchant_list table populated")


    tx_df = load_sheet("Descriptors")
    tx_df.columns = tx_df.columns.str.strip().str.lower().str.replace(" ", "_")

    print(f"{len(tx_df):,} rows read from 'Descriptors' sheet")

    tx_df.to_sql(
        name="descriptors",
        con=engine,
        if_exists="replace",      
        index=False,
        dtype={
            "descriptor_id":     types.String(36),
            "descriptor":        types.Text(),
            "cleaned_descriptor": types.Text(),
            "merchant_name":     types.String(255),
            "merchant_id":       types.String(36),
        },
    )
    print("✅ transactions table populated")


if __name__ == "__main__":
    main()
