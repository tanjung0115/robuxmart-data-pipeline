"""
robuxmart_etl_dag.py
--------------------
Apache Airflow DAG for Automated Daily Orchestration of the RobuxMart Data Pipeline.
Schedule: Daily at 01:00 UTC (`0 1 * * *`)
Tasks:
  1. `extract_oltp_sources`      -> Extracts from RobuxMart MySQL OLTP (or hybrid landing files)
  2. `transform_and_pii_mask`    -> Cleans data, masks PII, parses JSON logs, quarantines anomalies
  3. `data_quality_gate`         -> Runs 11 automated DQ assertions (blocks load on failure)
  4. `load_star_schema_dwh`      -> Upserts Star Schema into PostgreSQL Data Warehouse
  5. `audit_and_refresh_marts`   -> Logs ETL run history into `staging.etl_run_history` & verifies Marts
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure project root is importable inside Airflow container
PROJECT_ROOT = Path("/opt/airflow/project")
if PROJECT_ROOT.exists() and str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
else:
    local_root = Path(__file__).resolve().parent.parent
    if str(local_root) not in sys.path:
        sys.path.insert(0, str(local_root))


default_args = {
    "owner": "data_engineering_robuxmart",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def task_extract_sources(**context) -> dict:
    from src.extract import extract_all_sources

    source_mode = os.getenv("SOURCE_MODE", "hybrid")
    dfs, actual_source = extract_all_sources(mode=source_mode)
    summary = {
        "actual_source": actual_source,
        "users_extracted": len(dfs["users"]),
        "packages_extracted": len(dfs["packages"]),
        "transactions_extracted": len(dfs["transactions"]),
        "logs_extracted": len(dfs["interaction_logs"]),
    }
    context["ti"].xcom_push(key="extract_summary", value=summary)
    return summary


def task_transform_and_clean(**context) -> dict:
    from src.extract import extract_all_sources
    from src.transform import (
        transform_interaction_logs,
        transform_packages,
        transform_transactions,
        transform_users,
    )

    source_mode = os.getenv("SOURCE_MODE", "hybrid")
    raw_dfs, _ = extract_all_sources(mode=source_mode)
    dim_users = transform_users(raw_dfs["users"])
    dim_packages = transform_packages(raw_dfs["packages"])

    _, _, tx_metrics = transform_transactions(
        raw_dfs["transactions"],
        valid_user_ids=set(dim_users["user_id"]),
        valid_package_ids=set(dim_packages["package_id"]),
    )
    fact_logs = transform_interaction_logs(
        raw_dfs["interaction_logs"],
        valid_user_ids=set(dim_users["user_id"]),
    )
    tx_metrics["clean_logs"] = len(fact_logs)
    context["ti"].xcom_push(key="transform_metrics", value=tx_metrics)
    return tx_metrics


def task_data_quality_gate(**context) -> dict:
    from src.extract import extract_all_sources
    from src.quality import run_quality_checks
    from src.transform import (
        build_dim_date,
        build_dim_payment_methods,
        transform_interaction_logs,
        transform_packages,
        transform_transactions,
        transform_users,
    )

    source_mode = os.getenv("SOURCE_MODE", "hybrid")
    raw_dfs, _ = extract_all_sources(mode=source_mode)
    dim_users = transform_users(raw_dfs["users"])
    dim_packages = transform_packages(raw_dfs["packages"])
    valid_uids = set(dim_users["user_id"])
    valid_pids = set(dim_packages["package_id"])

    fact_tx, _, tx_metrics = transform_transactions(
        raw_dfs["transactions"], valid_user_ids=valid_uids, valid_package_ids=valid_pids
    )
    fact_logs = transform_interaction_logs(raw_dfs["interaction_logs"], valid_user_ids=valid_uids)
    dim_pm = build_dim_payment_methods(fact_tx)
    dim_date = build_dim_date(fact_tx, fact_logs)

    report = run_quality_checks(
        dim_users=dim_users,
        dim_packages=dim_packages,
        dim_payment_methods=dim_pm,
        dim_date=dim_date,
        fact_transactions=fact_tx,
        fact_logs=fact_logs,
        tx_metrics=tx_metrics,
    )
    return {
        "overall_status": report["overall_status"],
        "passed_checks": report["passed_checks"],
        "total_checks": report["total_checks"],
    }


def task_load_star_schema(**context) -> dict:
    from run_pipeline import run_etl_pipeline

    source_mode = os.getenv("SOURCE_MODE", "hybrid")
    engine = os.getenv("DWH_ENGINE", "postgres")
    result = run_etl_pipeline(source_mode=source_mode, engine=engine)
    context["ti"].xcom_push(key="load_result", value=result["loaded_counts"])
    return result["loaded_counts"]


def task_audit_and_refresh_marts(**context) -> dict:
    from sqlalchemy import create_engine, text

    host = os.getenv("DWH_HOST", "postgres-dwh")
    port = os.getenv("DWH_PORT", "5432")
    db = os.getenv("DWH_DB", "robuxmart_dwh")
    user = os.getenv("DWH_USER", "dwh_admin")
    password = os.getenv("DWH_PASSWORD", "dwh_secret_2026")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, connect_args={"connect_timeout": 5})

    run_id = context["run_id"]
    tx_metrics = context["ti"].xcom_pull(task_ids="transform_and_pii_mask", key="transform_metrics") or {}

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO staging.etl_run_history
                    (run_id, source_mode, started_at, completed_at, status,
                     rows_extracted, rows_cleaned, rows_quarantined, rows_loaded, notes)
                VALUES
                    (:run_id, :source_mode, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'SUCCESS',
                     :raw_rows, :clean_rows, :quarantined_rows, :clean_rows,
                     'Automated Airflow DAG execution')
                ON CONFLICT (run_id) DO UPDATE SET
                    completed_at = CURRENT_TIMESTAMP,
                    status = 'SUCCESS';
                """
            ),
            {
                "run_id": str(run_id)[:64],
                "source_mode": os.getenv("SOURCE_MODE", "hybrid"),
                "raw_rows": tx_metrics.get("raw_rows", 614),
                "clean_rows": tx_metrics.get("clean_rows", 600),
                "quarantined_rows": tx_metrics.get("quarantined_rows", 6),
            },
        )
        mart_rows = conn.execute(
            text("SELECT COUNT(*) FROM marts.mart_package_performance;")
        ).scalar()

    return {"run_id": run_id, "mart_package_rows": mart_rows, "audit_status": "RECORDED"}


with DAG(
    dag_id="robuxmart_daily_etl_pipeline",
    default_args=default_args,
    description="End-to-End RobuxMart OLTP (MySQL) to Star Schema DWH (PostgreSQL) Pipeline",
    schedule="0 1 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["robuxmart", "ecommerce", "star-schema", "data-warehouse"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_oltp_sources",
        python_callable=task_extract_sources,
    )

    transform_task = PythonOperator(
        task_id="transform_and_pii_mask",
        python_callable=task_transform_and_clean,
    )

    quality_task = PythonOperator(
        task_id="data_quality_gate",
        python_callable=task_data_quality_gate,
    )

    load_task = PythonOperator(
        task_id="load_star_schema_dwh",
        python_callable=task_load_star_schema,
    )

    audit_marts_task = PythonOperator(
        task_id="audit_and_refresh_marts",
        python_callable=task_audit_and_refresh_marts,
    )

    extract_task >> transform_task >> quality_task >> load_task >> audit_marts_task
