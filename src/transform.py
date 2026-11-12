"""
transform.py
------------
Data Cleaning, Standardization, PII Governance, JSON Log Parsing,
Dead-Letter Quarantine, and Star Schema Dimensional Modeling.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Set, Tuple

import pandas as pd

logger = logging.getLogger("robuxmart_etl.transform")

BASE_DIR = Path(__file__).resolve().parent.parent
QUARANTINE_DIR = BASE_DIR / "data" / "quarantine"

PAYMENT_CATEGORY_MAP = {
    "QRIS": ("QRIS (Quick Response Code)", "QR_CODE"),
    "GOPAY": ("GoPay E-Wallet", "E_WALLET"),
    "OVO": ("OVO E-Wallet", "E_WALLET"),
    "DANA": ("DANA E-Wallet", "E_WALLET"),
    "SHOPEEPAY": ("ShopeePay E-Wallet", "E_WALLET"),
    "BCA_VA": ("BCA Virtual Account", "VIRTUAL_ACCOUNT"),
    "MANDIRI_VA": ("Mandiri Virtual Account", "VIRTUAL_ACCOUNT"),
}

VALID_STATUSES = {"pending", "success", "failed"}


def mask_email(email: str) -> Tuple[str, str]:
    """Mask PII email address (`john.doe@gmail.com` -> `j***e@gmail.com`) and extract domain."""
    if not email or "@" not in str(email):
        return "unknown@masked.local", "masked.local"
    local, domain = str(email).strip().lower().split("@", 1)
    if len(local) <= 2:
        masked_local = local[0] + "***"
    else:
        masked_local = f"{local[0]}***{local[-1]}"
    return f"{masked_local}@{domain}", domain


def mask_phone(phone: Any) -> str:
    """Mask customer phone number (`081234567890` -> `0812****90`)."""
    s = str(phone or "").strip()
    if len(s) < 6:
        return "REDACTED"
    return f"{s[:4]}****{s[-2:]}"


def parse_user_agent(ua: str) -> Tuple[str, str]:
    """Extract browser and operating system from raw HTTP User-Agent string."""
    ua_str = str(ua or "")
    # OS detection
    if "Windows" in ua_str:
        os_name = "Windows"
    elif "Android" in ua_str:
        os_name = "Android"
    elif "iPhone" in ua_str or "iOS" in ua_str:
        os_name = "iOS"
    elif "Mac OS X" in ua_str or "Macintosh" in ua_str:
        os_name = "macOS"
    elif "Linux" in ua_str:
        os_name = "Linux"
    else:
        os_name = "Other"

    # Browser detection
    if "Firefox/" in ua_str:
        browser = "Firefox"
    elif "Chrome/" in ua_str:
        browser = "Chrome"
    elif "Safari/" in ua_str and "Chrome/" not in ua_str:
        browser = "Safari"
    else:
        browser = "Other"

    return browser, os_name


def summarize_json_details(raw_json: Any) -> str:
    """Safely parse semi-structured JSON `details` column from `interaction_logs`."""
    if not raw_json:
        return ""
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        if not isinstance(data, dict):
            return str(data)
        if "error" in data:
            return f"JS Error: {data.get('error')} ({data.get('stack', '')})"
        if "reason" in data:
            return f"Auth Fail: {data.get('reason')}"
        if "package" in data:
            return f"Checkout: {data.get('package')} (Rp {data.get('amount')})"
        if "page" in data:
            return f"View: {data.get('page')} ({data.get('duration_sec', data.get('duration', 0))}s)"
        if "email" in data:
            return f"Login IP: {data.get('ip', 'unknown')}"
        return json.dumps(data)
    except Exception:
        return str(raw_json)[:120]


def transform_users(df_users: pd.DataFrame) -> pd.DataFrame:
    """
    Clean `users` table and enforce PII Data Governance:
    - Drop sensitive `password` hash and `remember_token`
    - Mask `email` and `phone`
    - Derive `is_verified` boolean from `email_verified_at`
    """
    df = df_users.copy()
    df["user_id"] = df["id"].astype(int)
    df = df.drop_duplicates(subset=["user_id"], keep="first")

    df["name"] = df["name"].astype(str).str.strip().str.title()
    df["username"] = df["username"].astype(str).str.strip().str.lower()

    masked_pairs = df["email"].apply(mask_email)
    df["email_masked"] = [p[0] for p in masked_pairs]
    df["email_domain"] = [p[1] for p in masked_pairs]
    df["phone_masked"] = df["phone"].apply(mask_phone)

    df["is_admin"] = df["is_admin"].apply(lambda x: str(x).lower() in ("true", "1", "t"))
    df["is_verified"] = df["email_verified_at"].apply(lambda x: bool(str(x or "").strip()))
    df["registered_at"] = pd.to_datetime(df["created_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["updated_at"] = pd.to_datetime(df["updated_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    cols = [
        "user_id",
        "name",
        "username",
        "email_masked",
        "email_domain",
        "phone_masked",
        "is_admin",
        "is_verified",
        "registered_at",
        "updated_at",
    ]
    return df[cols].reset_index(drop=True)


def transform_packages(df_packages: pd.DataFrame) -> pd.DataFrame:
    """Clean `packages` table and enrich with `price_per_robux_idr` and `package_tier`."""
    df = df_packages.copy()
    df["package_id"] = df["id"].astype(int)
    df = df.drop_duplicates(subset=["package_id"], keep="first")

    df["package_name"] = df["name"].astype(str).str.strip()
    df["robux_amount"] = df["robux_amount"].astype(int)
    df["price_idr"] = df["price"].astype(float).round(2)
    df["price_per_robux_idr"] = (df["price_idr"] / df["robux_amount"]).round(4)

    def _assign_tier(robux: int) -> str:
        if robux < 1000:
            return "Entry"
        if robux < 5000:
            return "Mid-Tier"
        if robux <= 10000:
            return "High-Tier"
        return "VIP / Whale"

    df["package_tier"] = df["robux_amount"].apply(_assign_tier)
    df["is_active"] = df["is_active"].apply(lambda x: str(x).lower() in ("true", "1", "t"))
    df["updated_at"] = pd.to_datetime(df["updated_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    cols = [
        "package_id",
        "package_name",
        "robux_amount",
        "price_idr",
        "price_per_robux_idr",
        "package_tier",
        "is_active",
        "updated_at",
    ]
    return df[cols].reset_index(drop=True)


def transform_transactions(
    df_tx: pd.DataFrame,
    valid_user_ids: Set[int],
    valid_package_ids: Set[int],
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """
    Clean, deduplicate, and validate `transactions`.
    Invalid rows are routed to Dead-Letter Quarantine (`data/quarantine/rejected_transactions.csv`)
    with a clear `rejection_reason`.
    """
    raw_count = len(df_tx)
    df = df_tx.copy()
    df["transaction_id"] = df["id"].astype(int)

    # 1. Deduplicate webhook retries by transaction_id
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["transaction_id"], keep="first").copy()
    duplicates_removed = before_dedup - len(df)

    # 2. Normalize strings
    df["user_id"] = df["user_id"].astype(int)
    df["package_id"] = df["package_id"].astype(int)
    df["robux_amount"] = df["robux_amount"].astype(int)
    df["gross_amount_idr"] = df["price"].astype(float).round(2)
    df["roblox_id"] = df["roblox_id"].astype(str).str.strip()
    df["roblox_username"] = df["roblox_username"].astype(str).str.strip()
    df["payment_method_key"] = df["payment_method"].astype(str).str.strip().str.upper()
    df["status"] = df["status"].astype(str).str.strip().str.lower()

    # 3. Evaluate business & referential integrity rules
    reasons = []
    for _, row in df.iterrows():
        row_reasons = []
        if row["gross_amount_idr"] <= 0:
            row_reasons.append("INVALID_PRICE_LE_ZERO")
        if row["robux_amount"] <= 0:
            row_reasons.append("INVALID_ROBUX_AMOUNT_LE_ZERO")
        if not row["roblox_id"]:
            row_reasons.append("MISSING_ROBLOX_ID")
        if row["user_id"] not in valid_user_ids:
            row_reasons.append("ORPHAN_USER_ID")
        if row["package_id"] not in valid_package_ids:
            row_reasons.append("ORPHAN_PACKAGE_ID")
        if row["status"] not in VALID_STATUSES:
            row_reasons.append("INVALID_STATUS_ENUM")
        reasons.append("|".join(row_reasons))

    df["rejection_reason"] = reasons

    quarantined_df = df[df["rejection_reason"] != ""].copy()
    clean_df = df[df["rejection_reason"] == ""].copy()

    # Save quarantined records to disk for observability
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    quarantine_path = QUARANTINE_DIR / "rejected_transactions.csv"
    quarantined_df.to_csv(quarantine_path, index=False)

    # Enrich clean transactions for Star Schema
    clean_df["created_dt"] = pd.to_datetime(clean_df["created_at"])
    clean_df["date_key"] = clean_df["created_dt"].dt.strftime("%Y%m%d").astype(int)
    clean_df["created_at"] = clean_df["created_dt"].dt.strftime("%Y-%m-%d %H:%M:%S")
    clean_df["is_success"] = clean_df["status"] == "success"

    cols = [
        "transaction_id",
        "user_id",
        "package_id",
        "payment_method_key",
        "date_key",
        "roblox_id",
        "roblox_username",
        "robux_amount",
        "gross_amount_idr",
        "status",
        "is_success",
        "created_at",
    ]
    metrics = {
        "raw_rows": raw_count,
        "duplicates_removed": duplicates_removed,
        "quarantined_rows": len(quarantined_df),
        "clean_rows": len(clean_df),
    }
    logger.info("Transactions transformed: %s", metrics)
    return clean_df[cols].reset_index(drop=True), quarantined_df.reset_index(drop=True), metrics


def transform_interaction_logs(df_logs: pd.DataFrame, valid_user_ids: Set[int]) -> pd.DataFrame:
    """Transform semi-structured `interaction_logs` into structured fact records."""
    df = df_logs.copy()
    df["log_id"] = df["id"].astype(int)
    df = df.drop_duplicates(subset=["log_id"], keep="first")

    def _clean_uid(val: Any) -> Any:
        if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "none":
            return None
        uid = int(float(val))
        return uid if uid in valid_user_ids else None

    df["user_id"] = df["user_id"].apply(_clean_uid)
    df["action_type"] = df["action"].astype(str).str.strip().str.upper()

    ua_parsed = df["user_agent"].apply(parse_user_agent)
    df["browser"] = [p[0] for p in ua_parsed]
    df["os"] = [p[1] for p in ua_parsed]

    df["detail_summary"] = df["details"].apply(summarize_json_details)
    df["is_security_or_error"] = df["action_type"].isin({"LOGIN_FAILED", "ERROR_JS"})

    created_dt = pd.to_datetime(df["created_at"])
    df["date_key"] = created_dt.dt.strftime("%Y%m%d").astype(int)
    df["created_at"] = created_dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    cols = [
        "log_id",
        "user_id",
        "date_key",
        "action_type",
        "page_url",
        "ip_address",
        "browser",
        "os",
        "detail_summary",
        "is_security_or_error",
        "created_at",
    ]
    return df[cols].reset_index(drop=True)


def build_dim_payment_methods(df_clean_tx: pd.DataFrame) -> pd.DataFrame:
    """Build `dim_payment_methods` dimension table from standardized payment keys."""
    keys = sorted(df_clean_tx["payment_method_key"].unique())
    rows = []
    for k in keys:
        name, category = PAYMENT_CATEGORY_MAP.get(k, (k, "OTHER"))
        rows.append(
            {
                "payment_method_key": k,
                "payment_method_name": name,
                "payment_category": category,
            }
        )
    return pd.DataFrame(rows)


def build_dim_date(df_clean_tx: pd.DataFrame, df_clean_logs: pd.DataFrame) -> pd.DataFrame:
    """Build `dim_date` calendar dimension covering all transaction & event dates."""
    tx_dates = pd.to_datetime(df_clean_tx["created_at"]).dt.date
    log_dates = pd.to_datetime(df_clean_logs["created_at"]).dt.date
    min_date = min(tx_dates.min(), log_dates.min())
    max_date = max(tx_dates.max(), log_dates.max())

    date_range = pd.date_range(min_date, max_date, freq="D")
    rows = []
    for d in date_range:
        rows.append(
            {
                "date_key": int(d.strftime("%Y%m%d")),
                "full_date": d.strftime("%Y-%m-%d"),
                "year": d.year,
                "quarter": (d.month - 1) // 3 + 1,
                "month": d.month,
                "month_name": d.strftime("%B"),
                "day_of_month": d.day,
                "day_of_week": d.isoweekday(),
                "day_name": d.strftime("%A"),
                "is_weekend": d.isoweekday() in (6, 7),
            }
        )
    return pd.DataFrame(rows)
