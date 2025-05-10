# config.py
from dotenv import load_dotenv
import os

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")

# Sanity check
if not all([DB_USER, DB_PASS, DB_NAME]):
    raise RuntimeError(
        "Missing one or more required environment variables. "
        "Please check that .env contains DB_USER, DB_PASS, and DB_NAME."
    )
