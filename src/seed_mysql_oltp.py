"""
seed_mysql_oltp.py
------------------
Connects to the live RobuxMart MySQL 8.0 OLTP container (`robuxmart-mysql` from `C:\\fix devsec`)
and ensures the 4 application tables (`users`, `packages`, `transactions`, `interaction_logs`)
exist and are seeded with the RobuxMart transactional dataset so live MySQL -> PostgreSQL
extraction can be demonstrated end-to-end.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd
from sqlalchemy import create_engine, text

from src.generate_source_data import RAW_DIR, generate_all_source_files

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("robuxmart_etl.seed_mysql")


def seed_mysql_oltp() -> None:
    host = os.getenv("MYSQL_HOST", "localhost")
    port = os.getenv("MYSQL_PORT", "3306")
    db = os.getenv("MYSQL_DATABASE", "robuxmart")
    user = os.getenv("MYSQL_USER", "robuxmart")
    password = os.getenv("MYSQL_PASSWORD", "robuxmart_secure_2024")

    if not (RAW_DIR / "transactions.csv").exists():
        generate_all_source_files()

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, connect_args={"connect_timeout": 5})

    users_df = pd.read_csv(RAW_DIR / "users.csv", keep_default_na=False)
    packages_df = pd.read_csv(RAW_DIR / "packages.csv", keep_default_na=False)
    transactions_df = pd.read_csv(RAW_DIR / "transactions.csv", keep_default_na=False)
    with open(RAW_DIR / "interaction_logs.json", "r", encoding="utf-8") as f:
        logs_df = pd.DataFrame(json.load(f))

    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.execute(text("DROP TABLE IF EXISTS interaction_logs;"))
        conn.execute(text("DROP TABLE IF EXISTS transactions;"))
        conn.execute(text("DROP TABLE IF EXISTS packages;"))
        conn.execute(text("DROP TABLE IF EXISTS users;"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

        users_df.to_sql("users", conn, if_exists="replace", index=False)
        packages_df.to_sql("packages", conn, if_exists="replace", index=False)
        transactions_df.to_sql("transactions", conn, if_exists="replace", index=False)
        logs_df.to_sql("interaction_logs", conn, if_exists="replace", index=False)

    logger.info(
        "Seeded RobuxMart MySQL OLTP (%s:%s/%s) -> users=%d, packages=%d, transactions=%d, interaction_logs=%d",
        host,
        port,
        db,
        len(users_df),
        len(packages_df),
        len(transactions_df),
        len(logs_df),
    )


if __name__ == "__main__":
    seed_mysql_oltp()
