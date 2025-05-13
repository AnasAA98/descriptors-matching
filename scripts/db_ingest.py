# db_ingest.py
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, types, text
import config


WB_PATH       = Path("Descriptor_matching_assignment_for_ intership_applications.xlsx")
MERCHANT_DTYPE = {
    "merchant_id":    types.String(36),
    "merchant_name":  types.String(255),
    "merchant_city":  types.String(100),
    "merchant_state": types.String(100),
    "merchant_postal":types.String(20),
    "merchant_country": types.String(100),
}
DESC_DTYPE = {
    "descriptor_id":   types.String(36),
    "descriptor":      types.Text(),
    "merchant_name":   types.String(255),
    "merchant_id":     types.String(36),
}


def build_engine():
    """Return SQLAlchemy engine for fluz_db."""
    url = (
        f"mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASS}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )
    return create_engine(url, echo=False, future=True)


def load_sheet(sheet: str) -> pd.DataFrame:
    """Read a sheet as DataFrame and normalise column names."""
    df = pd.read_excel(WB_PATH, sheet_name=sheet, engine="openpyxl")
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(" ", "_", regex=False)
    )
    return df


# Main ETL
def main() -> None:
    engine = build_engine()

    # --- 1. Merchant List -------------------------------------------------- #
    merchants = load_sheet("Merchant List")
    print(f"📄  Merchant List: {len(merchants):,} rows")

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM merchant_list"))

    merchants.to_sql(
        "merchant_list", engine, if_exists="append", index=False, dtype=MERCHANT_DTYPE
    )
    print("✅ merchant_list table refreshed")

    # --- 2. Descriptors ---------------------------------------------------- #
    descriptors = load_sheet("Descriptors")
    print(f"📄  Descriptors   : {len(descriptors):,} rows")

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM descriptors"))

    descriptors.to_sql(
        "descriptors", engine, if_exists="append", index=False, dtype=DESC_DTYPE
    )
    print("✅ descriptors table refreshed")


if __name__ == "__main__":
    main()
