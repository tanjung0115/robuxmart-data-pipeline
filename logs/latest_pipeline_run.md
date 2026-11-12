# RobuxMart Data Pipeline — Latest Execution Report

- **Executed At:** `2026-11-12 04:34:17`
- **Source Mode:** `mysql`
- **Target Warehouse Engine:** `postgresql_dwh`
- **Execution Duration:** `1.764 seconds`
- **Data Quality Status:** `PASSED (11/11 checks passed)`

## 1. Ingestion & Data Cleaning Summary

| Metric | Count |
| :--- | ---: |
| Raw Transactions Extracted | 614 |
| Webhook Duplicates Removed | 8 |
| Corrupted Rows Quarantined (`rejected_transactions.csv`) | 6 |
| Clean Transactions Loaded to `fact_transactions` | 600 |

## 2. Quarantined Records (Dead-Letter Queue Audit)

| transaction_id | user_id | package_name | robux_amount | gross_amount_idr | status | rejection_reason |
| --- | --- | --- | --- | --- | --- | --- |
| 9001 | 2 | Paket Starter | 400 | -50000.0 | success | INVALID_PRICE_LE_ZERO |
| 9002 | 3 | Paket Basic | 800 | 0.0 | success | INVALID_PRICE_LE_ZERO |
| 9003 | 4 | Paket Popular | 1700 | 185000.0 | pending | MISSING_ROBLOX_ID |
| 9004 | 9999 | Paket Starter | 400 | 50000.0 | failed | ORPHAN_USER_ID |
| 9005 | 5 | Paket Premium | -4500 | 475000.0 | failed | INVALID_ROBUX_AMOUNT_LE_ZERO |
| 9006 | 6 | Paket Basic | 800 | 95000.0 | refunded_unknown | INVALID_STATUS_ENUM |

## 3. Star Schema Warehouse Row Counts

| Table Name | Role | Rows Loaded |
| :--- | :--- | ---: |
| `warehouse.dim_date` | Dimension | 30 |
| `warehouse.dim_users` | Dimension (PII Masked) | 31 |
| `warehouse.dim_packages` | Dimension | 6 |
| `warehouse.dim_payment_methods` | Dimension | 7 |
| `warehouse.fact_transactions` | Fact | 600 |
| `warehouse.fact_interaction_logs` | Fact (Parsed JSON Logs) | 1000 |

## 4. Analytical Data Mart Outputs

### A. Package Performance (`marts.mart_package_performance`)

| package_id | package_name | package_tier | package_robux | unit_price_idr | total_orders | completed_orders | total_robux_delivered | total_revenue_idr |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 6 | Paket VIP | VIP / Whale | 22500 | 1950000.0 | 27 | 25 | 562500 | 48750000.0 |
| 5 | Paket Ultimate | High-Tier | 10000 | 950000.0 | 46 | 38 | 380000 | 36100000.0 |
| 4 | Paket Premium | Mid-Tier | 4500 | 475000.0 | 75 | 54 | 243000 | 25650000.0 |
| 3 | Paket Popular | Mid-Tier | 1700 | 185000.0 | 145 | 113 | 192100 | 20905000.0 |
| 2 | Paket Basic | Entry | 800 | 95000.0 | 172 | 125 | 100000 | 11875000.0 |
| 1 | Paket Starter | Entry | 400 | 50000.0 | 135 | 106 | 42400 | 5300000.0 |

### B. Payment Gateway Health (`marts.mart_payment_method_health`)

| payment_method_name | payment_category | total_attempts | success_count | failed_count | pending_count | gateway_success_rate_pct | volume_settled_idr |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ShopeePay E-Wallet | E_WALLET | 89 | 63 | 17 | 9 | 70.79 | 26870000.0 |
| GoPay E-Wallet | E_WALLET | 90 | 73 | 7 | 10 | 81.11 | 24905000.0 |
| OVO E-Wallet | E_WALLET | 82 | 65 | 12 | 5 | 79.27 | 21800000.0 |
| BCA Virtual Account | VIRTUAL_ACCOUNT | 88 | 72 | 7 | 9 | 81.82 | 21540000.0 |
| QRIS (Quick Response Code) | QR_CODE | 73 | 59 | 10 | 4 | 80.82 | 19530000.0 |
| DANA E-Wallet | E_WALLET | 95 | 70 | 11 | 14 | 73.68 | 18885000.0 |
| Mandiri Virtual Account | VIRTUAL_ACCOUNT | 83 | 59 | 11 | 13 | 71.08 | 15050000.0 |

### C. Top 5 VIP Spenders with PII Masking

| user_id | name | email_masked | completed_tx | total_robux | lifetime_value_idr |
| --- | --- | --- | --- | --- | --- |
| 4 | Ahmad Santoso | a***d@example.com | 20 | 116100 | 11065000.0 |
| 27 | Sisca Kohl | s***i@gmail.com | 16 | 108600 | 9750000.0 |
| 14 | Arief Muhammad | a***h@gmail.com | 12 | 88200 | 7985000.0 |
| 17 | Windah Basudara | w***l@gmail.com | 22 | 72800 | 7370000.0 |
| 9 | Fajar Nugroho | f***o@gmail.com | 22 | 66600 | 6635000.0 |

### D. Security Anomaly Detection: Suspicious Failed Login IPs

| ip_address | browser | os | failed_login_attempts | first_seen | last_seen |
| --- | --- | --- | --- | --- | --- |
| 192.168.1.100 | Safari | macOS | 10 | 2026-10-16 10:38:00 | 2026-11-11 18:53:00 |
| 192.168.1.100 | Firefox | Windows | 9 | 2026-10-14 15:34:00 | 2026-11-08 08:22:00 |
| 192.168.1.100 | Chrome | Windows | 7 | 2026-10-19 05:59:00 | 2026-11-09 04:45:00 |
| 192.168.1.100 | Safari | iOS | 4 | 2026-10-18 23:56:00 | 2026-11-07 14:54:00 |
| 192.168.1.100 | Chrome | Android | 3 | 2026-10-17 12:04:00 | 2026-10-27 05:06:00 |
