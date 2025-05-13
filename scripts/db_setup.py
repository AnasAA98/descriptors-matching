# db_setup.py
from sqlalchemy import create_engine, text
import config

engine = create_engine(
    f"mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASS}"
    f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
)

with engine.begin() as conn:
    # 1. Merchant list 
    conn.execute(text("""
      CREATE TABLE IF NOT EXISTS merchant_list (
        merchant_id   VARCHAR(36) PRIMARY KEY,
        merchant_name VARCHAR(255) NOT NULL,
        merchant_city VARCHAR(100),
        merchant_state VARCHAR(100),
        merchant_postal VARCHAR(20),
        merchant_country VARCHAR(100)
      );
    """))
    # 2. descriptors 
    conn.execute(text("""
      CREATE TABLE IF NOT EXISTS descriptors (
        descriptor_id    VARCHAR(36) PRIMARY KEY,
        descriptor       TEXT NOT NULL,
        cleaned_descriptor TEXT,
        merchant_name    VARCHAR(255),
        merchant_id      VARCHAR(36),
        FOREIGN KEY (merchant_id) REFERENCES merchant_list(merchant_id)
      );
    """))
print("Schema created (or already existed).")
