# dump_db.py

from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import config


# SQLAlchemy engine
engine = create_engine(
    f"mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASS}"
    f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
)

OUT_DIR = Path.cwd()        
OUT_DIR.mkdir(exist_ok=True)


def dump_table(table: str) -> None:
    """Dump an entire table to CSV."""
    df = pd.read_sql(f"SELECT * FROM {table}", engine)
    path = OUT_DIR / f"{table}.csv"
    df.to_csv(path, index=False)
    print(f"✅  {table.ljust(14)}→ {path.name}  ({len(df):,} rows)")


def dump_unmatched() -> None:
    """Dump only the unmatched descriptors (merchant_id IS NULL)."""
    sql = text("SELECT * FROM descriptors WHERE merchant_id IS NULL")
    df = pd.read_sql(sql, engine)
    path = OUT_DIR / "data/unmatched.csv"
    df.to_csv(path, index=False)
    print(f"✅  unmatched       → {path.name}  ({len(df):,} rows)")


def main() -> None:
    for tbl in ("merchant_list", "descriptors"):
        dump_table(tbl)
    dump_unmatched()


if __name__ == "__main__":
    main()

