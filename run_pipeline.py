"""
run_pipeline.py
---------------
Main End-to-End Orchestrator for RobuxMart Data Engineering Pipeline (Phase 1).
Executes:
  Step 1: Hybrid Ingestion (MySQL OLTP / Raw Batch Files)
  Step 2: Data Cleaning, Normalization, PII Masking & Dead-Letter Quarantine
  Step 3: Automated Data Quality Gate (10 Assertions)
  Step 4: Idempotent Star Schema Load (PostgreSQL DWH / SQLite DWH)
  Step 5: Analytical Marts Execution & Markdown Report Generation
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

from src.extract import extract_all_sources
from src.load import load_star_schema, query_analytical_marts
from src.quality import run_quality_checks
from src.transform import (
    build_dim_date,
    build_dim_payment_methods,
    transform_interaction_logs,
    transform_packages,
    transform_transactions,
    transform_users,
)

BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "logs"


def setup_logging() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_etl_pipeline(source_mode: str = "hybrid", engine: str = "auto") -> dict:
    setup_logging()
    logger = logging.getLogger("robuxmart_etl.runner")
    t0 = time.perf_counter()

    logger.info("=================================================================")
    logger.info("STARTING ROBUXMART E-COMMERCE DATA PIPELINE (SOURCE=%s)", source_mode.upper())
    logger.info("=================================================================")

    # 1. EXTRACT
    raw_dfs, actual_source = extract_all_sources(mode=source_mode)

    # 2. TRANSFORM & PII GOVERNANCE
    dim_users = transform_users(raw_dfs["users"])
    dim_packages = transform_packages(raw_dfs["packages"])

    valid_uids = set(dim_users["user_id"])
    valid_pids = set(dim_packages["package_id"])

    fact_transactions, quarantined_tx, tx_metrics = transform_transactions(
        raw_dfs["transactions"],
        valid_user_ids=valid_uids,
        valid_package_ids=valid_pids,
    )
    fact_logs = transform_interaction_logs(raw_dfs["interaction_logs"], valid_user_ids=valid_uids)
    dim_payment_methods = build_dim_payment_methods(fact_transactions)
    dim_date = build_dim_date(fact_transactions, fact_logs)

    # 3. AUTOMATED DATA QUALITY GATE
    dq_report = run_quality_checks(
        dim_users=dim_users,
        dim_packages=dim_packages,
        dim_payment_methods=dim_payment_methods,
        dim_date=dim_date,
        fact_transactions=fact_transactions,
        fact_logs=fact_logs,
        tx_metrics=tx_metrics,
    )

    # 4. LOAD INTO DATA WAREHOUSE
    engine_used, loaded_counts = load_star_schema(
        dim_date=dim_date,
        dim_users=dim_users,
        dim_packages=dim_packages,
        dim_payment_methods=dim_payment_methods,
        fact_transactions=fact_transactions,
        fact_logs=fact_logs,
        preferred_engine=engine,
    )

    # 5. QUERY ANALYTICAL MARTS
    marts = query_analytical_marts()
    elapsed = round(time.perf_counter() - t0, 3)

    _write_markdown_report(
        actual_source=actual_source,
        engine_used=engine_used,
        elapsed=elapsed,
        tx_metrics=tx_metrics,
        quarantined_df=quarantined_tx,
        dq_report=dq_report,
        loaded_counts=loaded_counts,
        marts=marts,
    )

    logger.info("Pipeline completed in %.3fs (Engine=%s).", elapsed, engine_used)
    return {
        "source": actual_source,
        "engine": engine_used,
        "elapsed_sec": elapsed,
        "tx_metrics": tx_metrics,
        "dq_report": dq_report,
        "loaded_counts": loaded_counts,
    }


def _df_to_md(df) -> str:
    if df is None or df.empty:
        return "_No records._"
    cols = [str(c) for c in df.columns]
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = []
    for _, row in df.iterrows():
        body.append("| " + " | ".join(str(row[c]) for c in df.columns) + " |")
    return "\n".join([header, sep] + body)


def _write_markdown_report(
    actual_source: str,
    engine_used: str,
    elapsed: float,
    tx_metrics: dict,
    quarantined_df,
    dq_report: dict,
    loaded_counts: dict,
    marts: dict,
) -> None:
    report_path = LOGS_DIR / "latest_pipeline_run.md"
    lines = [
        "# RobuxMart Data Pipeline — Latest Execution Report",
        "",
        f"- **Executed At:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        f"- **Source Mode:** `{actual_source}`",
        f"- **Target Warehouse Engine:** `{engine_used}`",
        f"- **Execution Duration:** `{elapsed} seconds`",
        f"- **Data Quality Status:** `{dq_report['overall_status']} ({dq_report['passed_checks']}/{dq_report['total_checks']} checks passed)`",
        "",
        "## 1. Ingestion & Data Cleaning Summary",
        "",
        "| Metric | Count |",
        "| :--- | ---: |",
        f"| Raw Transactions Extracted | {tx_metrics['raw_rows']} |",
        f"| Webhook Duplicates Removed | {tx_metrics['duplicates_removed']} |",
        f"| Corrupted Rows Quarantined (`rejected_transactions.csv`) | {tx_metrics['quarantined_rows']} |",
        f"| Clean Transactions Loaded to `fact_transactions` | {tx_metrics['clean_rows']} |",
        "",
        "## 2. Quarantined Records (Dead-Letter Queue Audit)",
        "",
        _df_to_md(quarantined_df[["transaction_id", "user_id", "package_name", "robux_amount", "gross_amount_idr", "status", "rejection_reason"]]),
        "",
        "## 3. Star Schema Warehouse Row Counts",
        "",
        "| Table Name | Role | Rows Loaded |",
        "| :--- | :--- | ---: |",
        f"| `warehouse.dim_date` | Dimension | {loaded_counts['dim_date']} |",
        f"| `warehouse.dim_users` | Dimension (PII Masked) | {loaded_counts['dim_users']} |",
        f"| `warehouse.dim_packages` | Dimension | {loaded_counts['dim_packages']} |",
        f"| `warehouse.dim_payment_methods` | Dimension | {loaded_counts['dim_payment_methods']} |",
        f"| `warehouse.fact_transactions` | Fact | {loaded_counts['fact_transactions']} |",
        f"| `warehouse.fact_interaction_logs` | Fact (Parsed JSON Logs) | {loaded_counts['fact_interaction_logs']} |",
        "",
        "## 4. Analytical Data Mart Outputs",
        "",
        "### A. Package Performance (`marts.mart_package_performance`)",
        "",
        _df_to_md(marts["package_performance"]),
        "",
        "### B. Payment Gateway Health (`marts.mart_payment_method_health`)",
        "",
        _df_to_md(marts["payment_health"]),
        "",
        "### C. Top 5 VIP Spenders with PII Masking",
        "",
        _df_to_md(marts["top_spenders"]),
        "",
        "### D. Security Anomaly Detection: Suspicious Failed Login IPs",
        "",
        _df_to_md(marts["suspicious_ips"]),
        "",
    ]
    report_path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run RobuxMart End-to-End ETL Data Pipeline")
    parser.add_argument("--source", default="hybrid", choices=["hybrid", "mysql", "files"])
    parser.add_argument("--engine", default="auto", choices=["auto", "postgres", "sqlite"])
    args = parser.parse_args()
    run_etl_pipeline(source_mode=args.source, engine=args.engine)
