"""
test_pipeline.py
----------------
Automated Unit & Integration Test Suite for RobuxMart Data Pipeline.
Tests PII masking, deduplication, quarantine routing, JSON parsing, and end-to-end ETL.
"""

from __future__ import annotations

import pandas as pd

from run_pipeline import run_etl_pipeline
from src.transform import (
    mask_email,
    mask_phone,
    parse_user_agent,
    summarize_json_details,
    transform_transactions,
    transform_users,
)


def test_pii_masking_governance():
    masked, domain = mask_email("customer@robuxmart.com")
    assert masked == "c***r@robuxmart.com"
    assert domain == "robuxmart.com"
    assert "***" in masked

    masked_phone = mask_phone("081234567890")
    assert masked_phone == "0812****90"


def test_transform_users_drops_password():
    raw = pd.DataFrame(
        [
            {
                "id": 1,
                "name": "john doe",
                "username": "JohnDoe",
                "email": "john@example.com",
                "password": "$2y$10$secretpasswordhash",
                "phone": "081299998888",
                "is_admin": False,
                "email_verified_at": "2026-10-01 10:00:00",
                "created_at": "2026-10-01 09:00:00",
                "updated_at": "2026-10-01 09:00:00",
            }
        ]
    )
    cleaned = transform_users(raw)
    assert "password" not in cleaned.columns
    assert cleaned.loc[0, "name"] == "John Doe"
    assert cleaned.loc[0, "email_masked"] == "j***n@example.com"
    assert bool(cleaned.loc[0, "is_verified"]) is True


def test_transaction_deduplication_and_quarantine():
    raw_tx = pd.DataFrame(
        [
            # Valid row
            {
                "id": 100,
                "user_id": 2,
                "package_id": 1,
                "package_name": "Paket Starter",
                "robux_amount": 400,
                "price": 50000.0,
                "roblox_id": "12345",
                "roblox_username": "player1",
                "payment_method": " qris ",
                "payment_token": "TOK-100",
                "status": "success",
                "notes": "ok",
                "created_at": "2026-11-01 10:00:00",
                "updated_at": "2026-11-01 10:00:05",
            },
            # Exact webhook duplicate of id=100
            {
                "id": 100,
                "user_id": 2,
                "package_id": 1,
                "package_name": "Paket Starter",
                "robux_amount": 400,
                "price": 50000.0,
                "roblox_id": "12345",
                "roblox_username": "player1",
                "payment_method": "QRIS",
                "payment_token": "TOK-100",
                "status": "success",
                "notes": "duplicate retry",
                "created_at": "2026-11-01 10:00:00",
                "updated_at": "2026-11-01 10:00:05",
            },
            # Invalid negative price row -> should be quarantined
            {
                "id": 101,
                "user_id": 2,
                "package_id": 1,
                "package_name": "Paket Starter",
                "robux_amount": 400,
                "price": -50000.0,
                "roblox_id": "12345",
                "roblox_username": "player1",
                "payment_method": "gopay",
                "payment_token": "TOK-101",
                "status": "success",
                "notes": "bad price",
                "created_at": "2026-11-01 11:00:00",
                "updated_at": "2026-11-01 11:00:05",
            },
        ]
    )
    clean_df, quarantined_df, metrics = transform_transactions(
        raw_tx, valid_user_ids={2}, valid_package_ids={1}
    )
    assert metrics["duplicates_removed"] == 1
    assert metrics["quarantined_rows"] == 1
    assert metrics["clean_rows"] == 1
    assert clean_df.loc[0, "payment_method_key"] == "QRIS"
    assert "INVALID_PRICE_LE_ZERO" in quarantined_df.loc[0, "rejection_reason"]


def test_user_agent_and_json_parsing():
    browser, os_name = parse_user_agent(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
    )
    assert browser == "Chrome"
    assert os_name == "Windows"

    summary = summarize_json_details('{"package": "Paket VIP", "amount": 1950000}')
    assert "Paket VIP" in summary


def test_end_to_end_pipeline_execution():
    result = run_etl_pipeline(source_mode="files", engine="sqlite")
    assert result["dq_report"]["overall_status"] == "PASSED"
    assert result["tx_metrics"]["clean_rows"] == 600
    assert result["tx_metrics"]["duplicates_removed"] == 8
    assert result["tx_metrics"]["quarantined_rows"] == 6
