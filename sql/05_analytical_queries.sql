-- ============================================================================
-- 05_analytical_queries.sql
-- Ready-to-run analytical SQL queries demonstrating Window Functions, CTEs,
-- Cohort/Customer Segmentation, and Security Anomaly Detection.
-- ============================================================================

-- 1. Executive Summary: Package Revenue Share & Ranking (Window Function)
SELECT
    package_name,
    package_tier,
    completed_orders,
    total_robux_delivered,
    total_revenue_idr,
    revenue_share_pct,
    RANK() OVER (ORDER BY total_revenue_idr DESC) AS revenue_rank
FROM marts.mart_package_performance;

-- 2. Payment Method Reliability (Identifying Failing Gateways)
SELECT
    payment_method_name,
    payment_category,
    total_attempts,
    success_count,
    failed_count,
    gateway_success_rate_pct,
    volume_settled_idr
FROM marts.mart_payment_method_health
ORDER BY gateway_success_rate_pct ASC;

-- 3. Top 10 High-Value Customers (VIP Spenders with Masked PII)
WITH customer_spend AS (
    SELECT
        u.user_id,
        u.name,
        u.email_masked,
        COUNT(f.transaction_id) AS total_tx,
        SUM(CASE WHEN f.is_success THEN 1 ELSE 0 END) AS successful_tx,
        SUM(CASE WHEN f.is_success THEN f.robux_amount ELSE 0 END) AS total_robux_bought,
        SUM(CASE WHEN f.is_success THEN f.gross_amount_idr ELSE 0 END) AS lifetime_value_idr
    FROM warehouse.dim_users u
    JOIN warehouse.fact_transactions f ON u.user_id = f.user_id
    WHERE u.is_admin = FALSE
    GROUP BY u.user_id, u.name, u.email_masked
)
SELECT
    user_id,
    name,
    email_masked,
    successful_tx,
    total_robux_bought,
    lifetime_value_idr,
    CASE
        WHEN lifetime_value_idr >= 2000000 THEN 'Whale / VIP'
        WHEN lifetime_value_idr >= 500000 THEN 'Regular Buyer'
        ELSE 'Casual Buyer'
    END AS spender_segment
FROM customer_spend
WHERE lifetime_value_idr > 0
ORDER BY lifetime_value_idr DESC
LIMIT 10;

-- 4. Security & Anomaly Monitoring: Brute-Force Login Attempts by IP
SELECT
    ip_address,
    browser,
    os,
    COUNT(*) AS failed_login_attempts,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS last_seen
FROM warehouse.fact_interaction_logs
WHERE action_type = 'LOGIN_FAILED'
GROUP BY ip_address, browser, os
HAVING COUNT(*) >= 2
ORDER BY failed_login_attempts DESC;
