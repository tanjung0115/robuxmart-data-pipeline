-- ============================================================================
-- 04_data_marts.sql
-- Business Intelligence & Security Analytics Views (marts schema)
-- ============================================================================

-- Mart 1: Daily Revenue & Order Conversion Summary
CREATE OR REPLACE VIEW marts.mart_daily_revenue AS
SELECT
    d.full_date AS transaction_date,
    d.day_name,
    d.is_weekend,
    COUNT(f.transaction_id) AS total_transactions,
    SUM(CASE WHEN f.status = 'success' THEN 1 ELSE 0 END) AS successful_tx,
    SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END) AS failed_tx,
    SUM(CASE WHEN f.status = 'pending' THEN 1 ELSE 0 END) AS pending_tx,
    ROUND(
        100.0 * SUM(CASE WHEN f.status = 'success' THEN 1 ELSE 0 END) / NULLIF(COUNT(f.transaction_id), 0),
        2
    ) AS success_rate_pct,
    COALESCE(SUM(CASE WHEN f.status = 'success' THEN f.robux_amount ELSE 0 END), 0) AS total_robux_sold,
    COALESCE(SUM(CASE WHEN f.status = 'success' THEN f.gross_amount_idr ELSE 0 END), 0) AS net_revenue_idr,
    COALESCE(AVG(CASE WHEN f.status = 'success' THEN f.gross_amount_idr END), 0)::NUMERIC(12,2) AS avg_order_value_idr
FROM warehouse.fact_transactions f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date DESC;

-- Mart 2: Robux Package Performance & Tier Ranking
CREATE OR REPLACE VIEW marts.mart_package_performance AS
SELECT
    p.package_id,
    p.package_name,
    p.package_tier,
    p.robux_amount AS package_robux,
    p.price_idr AS unit_price_idr,
    COUNT(f.transaction_id) AS total_orders,
    SUM(CASE WHEN f.is_success THEN 1 ELSE 0 END) AS completed_orders,
    COALESCE(SUM(CASE WHEN f.is_success THEN f.robux_amount ELSE 0 END), 0) AS total_robux_delivered,
    COALESCE(SUM(CASE WHEN f.is_success THEN f.gross_amount_idr ELSE 0 END), 0) AS total_revenue_idr,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_success THEN f.gross_amount_idr ELSE 0 END)
        / NULLIF(SUM(SUM(CASE WHEN f.is_success THEN f.gross_amount_idr ELSE 0 END)) OVER (), 0),
        2
    ) AS revenue_share_pct
FROM warehouse.dim_packages p
LEFT JOIN warehouse.fact_transactions f ON p.package_id = f.package_id
GROUP BY p.package_id, p.package_name, p.package_tier, p.robux_amount, p.price_idr
ORDER BY total_revenue_idr DESC;

-- Mart 3: Payment Method Reliability & Gateway Health
CREATE OR REPLACE VIEW marts.mart_payment_method_health AS
SELECT
    pm.payment_method_name,
    pm.payment_category,
    COUNT(f.transaction_id) AS total_attempts,
    SUM(CASE WHEN f.status = 'success' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN f.status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.status = 'success' THEN 1 ELSE 0 END) / NULLIF(COUNT(f.transaction_id), 0),
        2
    ) AS gateway_success_rate_pct,
    COALESCE(SUM(CASE WHEN f.status = 'success' THEN f.gross_amount_idr ELSE 0 END), 0) AS volume_settled_idr
FROM warehouse.dim_payment_methods pm
JOIN warehouse.fact_transactions f ON pm.payment_method_key = f.payment_method_key
GROUP BY pm.payment_method_name, pm.payment_category
ORDER BY volume_settled_idr DESC;

-- Mart 4: User Funnel & Security Event Summary (DevSecOps + Product Analytics)
CREATE OR REPLACE VIEW marts.mart_funnel_and_security AS
SELECT
    l.action_type,
    l.is_security_or_error,
    COUNT(l.log_id) AS event_count,
    COUNT(DISTINCT l.user_id) AS unique_logged_in_users,
    COUNT(DISTINCT l.ip_address) AS distinct_ips
FROM warehouse.fact_interaction_logs l
GROUP BY l.action_type, l.is_security_or_error
ORDER BY event_count DESC;
