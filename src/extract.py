"""
extract.py
----------
Hybrid Data Ingestion Layer for RobuxMart Data Pipeline.
Supports extracting from:
1. Live RobuxMart MySQL 8.0 OLTP database (`robuxmart-mysql` container)
2. Raw CSV / JSON batch landing files in `data/raw/` (auto-generated if absent)
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from src.generate_source_data import RAW_DIR, generate_all_source_files

logger = logging.getLogger("robuxmart_etl.extract")


def extract_all_sources(mode: str = "hybrid") -> Tuple[Dict[str, pd.DataFrame], str]:
    """
    Extract `users`, `packages`, `transactions`, and `interaction_logs`.
    Returns a tuple of (dataframes_dict, actual_source_used).
    """
    mode = mode.lower().strip()

    if mode in ("mysql", "hybrid"):
        try:
            dfs = _extract_from_mysql()
            logger.info("Successfully extracted live OLTP data from MySQL container.")
            return dfs, "mysql"
        except Exception as exc:
            if mode == "mysql":
                raise RuntimeError(f"Failed to connect to RobuxMart MySQL: {exc}") from exc
            logger.info(
                "MySQL OLTP container not reachable (%s). Falling back to hybrid raw landing files.",
                type(exc).__name__,
            )

    # Ensure raw landing files exist
    required_files = [
        RAW_DIR / "users.csv",
        RAW_DIR / "packages.csv",
        RAW_DIR / "transactions.csv",
        RAW_DIR / "interaction_logs.json",
    ]
    if not all(p.exists() for p in required_files):
        logger.info("Raw source files not found in %s. Generating realistic RobuxMart dataset...", RAW_DIR)
        generate_all_source_files()

    dfs = _extract_from_files()
    return dfs, "files_hybrid"


def _extract_from_mysql() -> Dict[str, pd.DataFrame]:
    """Connect to RobuxMart MySQL 8.0 and extract the 4 core OLTP tables."""
    from sqlalchemy import create_engine

    host = os.getenv("MYSQL_HOST", "localhost")
    port = os.getenv("MYSQL_PORT", "3306")
    db = os.getenv("MYSQL_DATABASE", "robuxmart")
    user = os.getenv("MYSQL_USER", "robuxmart")
    password = os.getenv("MYSQL_PASSWORD", "robuxmart_secure_2024")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, connect_args={"connect_timeout": 3})

    with engine.connect() as conn:
        users_df = pd.read_sql("SELECT * FROM users", conn)
        packages_df = pd.read_sql("SELECT * FROM packages", conn)
        transactions_df = pd.read_sql("SELECT * FROM transactions", conn)
        logs_df = pd.read_sql("SELECT * FROM interaction_logs", conn)

    return {
        "users": users_df,
        "packages": packages_df,
        "transactions": transactions_df,
        "interaction_logs": logs_df,
    }


def _extract_from_files() -> Dict[str, pd.DataFrame]:
    """Extract raw datasets from CSV and JSON files in `data/raw/`."""
    users_df = pd.read_csv(RAW_DIR / "users.csv", keep_default_na=False)
    packages_df = pd.read_csv(RAW_DIR / "packages.csv", keep_default_na=False)
    transactions_df = pd.read_csv(RAW_DIR / "transactions.csv", keep_default_na=False)

    with open(RAW_DIR / "interaction_logs.json", "r", encoding="utf-8") as f:
        logs_data = json.load(f)
    logs_df = pd.DataFrame(logs_data)

    logger.info(
        "Extracted raw batch files -> users=%d, packages=%d, transactions=%d, interaction_logs=%d",
        len(users_df),
        len(packages_df),
        len(transactions_df),
        len(logs_df),
    )
    return {
        "users": users_df,
        "packages": packages_df,
        "transactions": transactions_df,
        "interaction_logs": logs_df,
    }
