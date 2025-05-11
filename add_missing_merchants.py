import uuid
import pandas as pd
from sqlalchemy import create_engine, text
import config

def build_engine():
    url = (
        f"mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASS}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )
    return create_engine(url, echo=False, future=True)

def main():
    aliases = pd.read_csv("aliases.csv")
    needed = {c.upper() for c in aliases.canonical}

    engine = build_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT merchant_name FROM merchant_list"))
        existing = {r.merchant_name.upper() for r in rows}

        add_list = sorted(needed - existing)
        if not add_list:
            print("✅ merchant_list already has every alias target.")
            return

        print("Adding:", ", ".join(add_list))
        for name in add_list:
            conn.execute(
                text("INSERT INTO merchant_list (merchant_id, merchant_name) VALUES (:mid, :mname)"),
                {"mid": str(uuid.uuid4()), "mname": name}
            )

    print("✅ merchant_list updated.")
    
if __name__ == "__main__":
    main()
