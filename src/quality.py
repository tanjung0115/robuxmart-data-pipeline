"""
quality.py
----------
Automated Data Quality Gate for the RobuxMart Data Warehouse.
Verifies primary key uniqueness, non-null constraints, referential integrity,
domain value ranges, and PII governance compliance prior to warehouse load.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger("robuxmart_etl.quality")

BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / "logs"


def run_quality_checks(
    dim_users: pd.DataFrame,
    dim_packages: pd.DataFrame,
    dim_payment_methods: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_transactions: pd.DataFrame,
    fact_logs: pd.DataFrame,
    tx_metrics: Dict[str, int],
) -> Dict[str, Any]:
    """Execute 10 automated data quality assertions and persist JSON audit report."""
    checks: List[Dict[str, Any]] = []

    def _add_check(name: str, passed: bool, details: str) -> None:
        checks.append(
            {
                "check_name": name,
                "status": "PASSED" if passed else "FAILED",
                "passed": bool(passed),
                "details": details,
            }
        )

    # 1. Primary Key Uniqueness
    _add_check(
        "pk_unique_dim_users",
        dim_users["user_id"].is_unique,
        f"Unique user_ids: {dim_users['user_id'].nunique()} / {len(dim_users)}",
    )
    _add_check(
        "pk_unique_fact_transactions",
        fact_transactions["transaction_id"].is_unique,
        f"Unique transaction_ids: {fact_transactions['transaction_id'].nunique()} / {len(fact_transactions)} "
        f"(Removed {tx_metrics.get('duplicates_removed', 0)} webhook duplicates)",
    )

    # 2. PII Governance Check (No raw emails or passwords in dim_users)
    has_no_password = "password" not in dim_users.columns
    all_emails_masked = dim_users["email_masked"].str.contains(r"\*\*\*", regex=True).all()
    _add_check(
        "pii_governance_dim_users",
        has_no_password and all_emails_masked,
        "Password column dropped; 100% of user emails masked with '***'",
    )

    # 3. Positive Monetary & Robux Amounts in Fact Transactions
    positive_amounts = (
        (fact_transactions["gross_amount_idr"] > 0).all()
        and (fact_transactions["robux_amount"] > 0).all()
    )
    _add_check(
        "positive_transaction_amounts",
        positive_amounts,
        f"Min price={fact_transactions['gross_amount_idr'].min()}, "
        f"Min robux={fact_transactions['robux_amount'].min()} "
        f"(Quarantined {tx_metrics.get('quarantined_rows', 0)} invalid records)",
    )

    # 4. Valid Transaction Status Enum
    valid_statuses = set(fact_transactions["status"].unique()).issubset({"pending", "success", "failed"})
    _add_check(
        "valid_transaction_status_enum",
        valid_statuses,
        f"Observed statuses: {sorted(fact_transactions['status'].unique())}",
    )

    # 5. Referential Integrity: fact_transactions -> dim_users
    orphan_users = set(fact_transactions["user_id"]) - set(dim_users["user_id"])
    _add_check(
        "fk_integrity_tx_users",
        len(orphan_users) == 0,
        f"Orphan user_ids in fact_transactions: {len(orphan_users)}",
    )

    # 6. Referential Integrity: fact_transactions -> dim_packages
    orphan_pkgs = set(fact_transactions["package_id"]) - set(dim_packages["package_id"])
    _add_check(
        "fk_integrity_tx_packages",
        len(orphan_pkgs) == 0,
        f"Orphan package_ids in fact_transactions: {len(orphan_pkgs)}",
    )

    # 7. Referential Integrity: fact_transactions -> dim_payment_methods
    orphan_pm = set(fact_transactions["payment_method_key"]) - set(dim_payment_methods["payment_method_key"])
    _add_check(
        "fk_integrity_tx_payment_methods",
        len(orphan_pm) == 0,
        f"Orphan payment_method_keys: {len(orphan_pm)}",
    )

    # 8. Referential Integrity: fact_transactions -> dim_date
    orphan_dates = set(fact_transactions["date_key"]) - set(dim_date["date_key"])
    _add_check(
        "fk_integrity_tx_date",
        len(orphan_dates) == 0,
        f"Orphan date_keys in fact_transactions: {len(orphan_dates)}",
    )

    # 9. Mandatory Destination Roblox ID Non-Empty
    non_empty_roblox = (fact_transactions["roblox_id"].str.len() > 0).all()
    _add_check(
        "non_empty_roblox_id",
        non_empty_roblox,
        "100% of loaded transactions have a valid target roblox_id",
    )

    # 10. Non-Empty Warehouse Volume Check
    volume_ok = len(fact_transactions) >= 100 and len(fact_logs) >= 100
    _add_check(
        "minimum_warehouse_volume",
        volume_ok,
        f"Loaded {len(fact_transactions)} transactions and {len(fact_logs)} interaction logs",
    )

    all_passed = all(c["passed"] for c in checks)
    report = {
        "evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "overall_status": "PASSED" if all_passed else "FAILED",
        "total_checks": len(checks),
        "passed_checks": sum(1 for c in checks if c["passed"]),
        "failed_checks": sum(1 for c in checks if not c["passed"]),
        "transaction_cleaning_metrics": tx_metrics,
        "checks": checks,
    }

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = LOGS_DIR / "data_quality_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    if not all_passed:
        raise ValueError(f"Data Quality Gate FAILED! Inspect {report_path}")

    logger.info("Data Quality Gate PASSED (%d/%d checks).", report["passed_checks"], report["total_checks"])
    return report
