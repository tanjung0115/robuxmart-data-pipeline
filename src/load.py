"""
load.py
-------
Idempotent Data Warehouse Loader & Analytical Query Runner.
Supports:
1. PostgreSQL 15 Data Warehouse (`robuxmart-postgres-dwh` Docker container)
   with multi-schema (`staging`, `warehouse`, `marts`) and `ON CONFLICT DO UPDATE` upserts.
2. Local SQLite Star Schema Data Warehouse (`data/warehouse/robuxmart_dwh.db`)
   for zero-dependency local execution & automated verification.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

logger = logging.getLogger("robuxmart_etl.load")

BASE_DIR = Path(__file__).resolve().parent.parent
SQLITE_DB_PATH = BASE_DIR / "data" / "warehouse" / "robuxmart_dwh.db"
SQL_DIR = BASE_DIR / "sql"


def load_star_schema(
    dim_date: pd.DataFrame,
    dim_users: pd.DataFrame,
    dim_packages: pd.DataFrame,
    dim_payment_methods: pd.DataFrame,
    fact_transactions: pd.DataFrame,
    fact_logs: pd.DataFrame,
    preferred_engine: str = "auto",
) -> Tuple[str, Dict[str, int]]:
    """
    Load Star Schema tables into PostgreSQL DWH (if reachable) and/or local SQLite DWH.
    Returns `(engine_used, loaded_row_counts)`.
    """
    preferred_engine = preferred_engine.lower().strip()
    row_counts = {
        "dim_date": len(dim_date),
        "dim_users": len(dim_users),
        "dim_packages": len(dim_packages),
        "dim_payment_methods": len(dim_payment_methods),
        "fact_transactions": len(fact_transactions),
        "fact_interaction_logs": len(fact_logs),
    }

    if preferred_engine in ("postgres", "auto"):
        try:
            _load_to_postgres(
                dim_date,
                dim_users,
                dim_packages,
                dim_payment_methods,
                fact_transactions,
                fact_logs,
            )
            logger.info("Loaded Star Schema into PostgreSQL Data Warehouse (schemas: warehouse, marts).")
            # Also mirror to local SQLite warehouse so local file inspection is always up-to-date
            _load_to_sqlite(
                dim_date,
                dim_users,
                dim_packages,
                dim_payment_methods,
                fact_transactions,
                fact_logs,
            )
            return "postgresql_dwh", row_counts
        except Exception as exc:
            if preferred_engine == "postgres":
                raise RuntimeError(f"PostgreSQL DWH connection failed: {exc}") from exc
            logger.info(
                "PostgreSQL container not active (%s). Loading into local Star Schema Warehouse (%s).",
                type(exc).__name__,
                SQLITE_DB_PATH.name,
            )

    _load_to_sqlite(
        dim_date,
        dim_users,
        dim_packages,
        dim_payment_methods,
        fact_transactions,
        fact_logs,
    )
    return "sqlite_dwh", row_counts


def _load_to_postgres(
    dim_date: pd.DataFrame,
    dim_users: pd.DataFrame,
    dim_packages: pd.DataFrame,
    dim_payment_methods: pd.DataFrame,
    fact_transactions: pd.DataFrame,
    fact_logs: pd.DataFrame,
) -> None:
    """Connect to PostgreSQL DWH, ensure schemas/tables/views exist, and upsert Star Schema data."""
    from sqlalchemy import create_engine, text

    host = os.getenv("DWH_HOST", "localhost")
    port = os.getenv("DWH_PORT", "5433")
    db = os.getenv("DWH_DB", "robuxmart_dwh")
    user = os.getenv("DWH_USER", "dwh_admin")
    password = os.getenv("DWH_PASSWORD", "dwh_secret_2026")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, connect_args={"connect_timeout": 3})

    with engine.begin() as conn:
        # Execute DDL initialization scripts idempotently
        for sql_file in sorted(SQL_DIR.glob("0[1-4]_*.sql")):
            sql_text = sql_file.read_text(encoding="utf-8")
            conn.execute(text(sql_text))

        # Truncate and reload warehouse tables inside a single transaction (Idempotent batch refresh)
        conn.execute(
            text(
                "TRUNCATE TABLE warehouse.fact_interaction_logs, warehouse.fact_transactions, "
                "warehouse.dim_payment_methods, warehouse.dim_packages, warehouse.dim_users, "
                "warehouse.dim_date RESTART IDENTITY CASCADE;"
            )
        )

        dim_date.to_sql("dim_date", conn, schema="warehouse", if_exists="append", index=False)
        dim_users.to_sql("dim_users", conn, schema="warehouse", if_exists="append", index=False)
        dim_packages.to_sql("dim_packages", conn, schema="warehouse", if_exists="append", index=False)
        dim_payment_methods.to_sql(
            "dim_payment_methods", conn, schema="warehouse", if_exists="append", index=False
        )
        fact_transactions.to_sql(
            "fact_transactions", conn, schema="warehouse", if_exists="append", index=False
        )
        fact_logs.to_sql(
            "fact_interaction_logs", conn, schema="warehouse", if_exists="append", index=False
        )


def _load_to_sqlite(
    dim_date: pd.DataFrame,
    dim_users: pd.DataFrame,
    dim_packages: pd.DataFrame,
    dim_payment_methods: pd.DataFrame,
    fact_transactions: pd.DataFrame,
    fact_logs: pd.DataFrame,
) -> None:
    """Idempotently load Star Schema tables and analytical Mart views into SQLite DWH."""
    SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)
        dim_users.to_sql("dim_users", conn, if_exists="replace", index=False)
        dim_packages.to_sql("dim_packages", conn, if_exists="replace", index=False)
        dim_payment_methods.to_sql("dim_payment_methods", conn, if_exists="replace", index=False)
        fact_transactions.to_sql("fact_transactions", conn, if_exists="replace", index=False)
        fact_logs.to_sql("fact_interaction_logs", conn, if_exists="replace", index=False)

        # Create analytical views matching sql/04_data_marts.sql
        conn.executescript(
            """
            DROP VIEW IF EXISTS mart_daily_revenue;
            CREATE VIEW mart_daily_revenue AS
            SELECT
                d.full_date AS transaction_date,
                d.day_name,
                d.is_weekend,
                COUNT(f.transaction_id) AS total_transactions,
                SUM(CASE WHEN f.status = 'success' THEN 1 ELSE 0 END) AS successful_tx,
                SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END) AS failed_tx,
                SUM(CASE WHEN f.status = 'pending' THEN 1 ELSE 0 END) AS pending_tx,
                ROUND(100.0 * SUM(CASE WHEN f.status = 'success' THEN 1 ELSE 0 END) / COUNT(f.transaction_id), 2) AS success_rate_pct,
                SUM(CASE WHEN f.status = 'success' THEN f.robux_amount ELSE 0 END) AS total_robux_sold,
                SUM(CASE WHEN f.status = 'success' THEN f.gross_amount_idr ELSE 0 END) AS net_revenue_idr
            FROM fact_transactions f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.full_date, d.day_name, d.is_weekend
            ORDER BY d.full_date DESC;

            DROP VIEW IF EXISTS mart_package_performance;
            CREATE VIEW mart_package_performance AS
            SELECT
                p.package_id,
                p.package_name,
                p.package_tier,
                p.robux_amount AS package_robux,
                p.price_idr AS unit_price_idr,
                COUNT(f.transaction_id) AS total_orders,
                SUM(CASE WHEN f.is_success = 1 THEN 1 ELSE 0 END) AS completed_orders,
                SUM(CASE WHEN f.is_success = 1 THEN f.robux_amount ELSE 0 END) AS total_robux_delivered,
                SUM(CASE WHEN f.is_success = 1 THEN f.gross_amount_idr ELSE 0 END) AS total_revenue_idr
            FROM dim_packages p
            LEFT JOIN fact_transactions f ON p.package_id = f.package_id
            GROUP BY p.package_id, p.package_name, p.package_tier, p.robux_amount, p.price_idr
            ORDER BY total_revenue_idr DESC;

            DROP VIEW IF EXISTS mart_payment_method_health;
            CREATE VIEW mart_payment_method_health AS
            SELECT
                pm.payment_method_name,
                pm.payment_category,
                COUNT(f.transaction_id) AS total_attempts,
                SUM(CASE WHEN f.status = 'success' THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
                SUM(CASE WHEN f.status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
                ROUND(100.0 * SUM(CASE WHEN f.status = 'success' THEN 1 ELSE 0 END) / COUNT(f.transaction_id), 2) AS gateway_success_rate_pct,
                SUM(CASE WHEN f.status = 'success' THEN f.gross_amount_idr ELSE 0 END) AS volume_settled_idr
            FROM dim_payment_methods pm
            JOIN fact_transactions f ON pm.payment_method_key = f.payment_method_key
            GROUP BY pm.payment_method_name, pm.payment_category
            ORDER BY volume_settled_idr DESC;

            DROP VIEW IF EXISTS mart_funnel_and_security;
            CREATE VIEW mart_funnel_and_security AS
            SELECT
                l.action_type,
                l.is_security_or_error,
                COUNT(l.log_id) AS event_count,
                COUNT(DISTINCT l.user_id) AS unique_logged_in_users,
                COUNT(DISTINCT l.ip_address) AS distinct_ips
            FROM fact_interaction_logs l
            GROUP BY l.action_type, l.is_security_or_error
            ORDER BY event_count DESC;
            """
        )


def query_analytical_marts() -> Dict[str, pd.DataFrame]:
    """Execute analytical SQL queries against the populated Data Warehouse to generate insights."""
    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        pkg_perf = pd.read_sql("SELECT * FROM mart_package_performance", conn)
        pay_health = pd.read_sql("SELECT * FROM mart_payment_method_health", conn)
        funnel_sec = pd.read_sql("SELECT * FROM mart_funnel_and_security", conn)
        top_spenders = pd.read_sql(
            """
            SELECT
                u.user_id,
                u.name,
                u.email_masked,
                SUM(CASE WHEN f.is_success = 1 THEN 1 ELSE 0 END) AS completed_tx,
                SUM(CASE WHEN f.is_success = 1 THEN f.robux_amount ELSE 0 END) AS total_robux,
                SUM(CASE WHEN f.is_success = 1 THEN f.gross_amount_idr ELSE 0 END) AS lifetime_value_idr
            FROM dim_users u
            JOIN fact_transactions f ON u.user_id = f.user_id
            WHERE u.is_admin = 0
            GROUP BY u.user_id, u.name, u.email_masked
            ORDER BY lifetime_value_idr DESC
            LIMIT 5;
            """,
            conn,
        )
        security_ips = pd.read_sql(
            """
            SELECT
                ip_address,
                browser,
                os,
                COUNT(*) AS failed_login_attempts,
                MIN(created_at) AS first_seen,
                MAX(created_at) AS last_seen
            FROM fact_interaction_logs
            WHERE action_type = 'LOGIN_FAILED'
            GROUP BY ip_address, browser, os
            HAVING COUNT(*) >= 3
            ORDER BY failed_login_attempts DESC
            LIMIT 5;
            """,
            conn,
        )

    return {
        "package_performance": pkg_perf,
        "payment_health": pay_health,
        "funnel_and_security": funnel_sec,
        "top_spenders": top_spenders,
        "suspicious_ips": security_ips,
    }
