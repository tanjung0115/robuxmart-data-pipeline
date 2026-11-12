-- ============================================================================
-- 03_warehouse_star_schema.sql
-- Dimensional Modeling (Star Schema) for RobuxMart Data Warehouse
-- ============================================================================

-- 1. Dimension: Date (warehouse.dim_date)
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,          -- Format: YYYYMMDD (e.g., 20261112)
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- 2. Dimension: Users with PII Governance (warehouse.dim_users)
CREATE TABLE IF NOT EXISTS warehouse.dim_users (
    user_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    username VARCHAR(255),
    email_masked VARCHAR(255) NOT NULL,
    email_domain VARCHAR(120) NOT NULL,
    phone_masked VARCHAR(64),
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    is_verified BOOLEAN NOT NULL DEFAULT FALSE,
    registered_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Dimension: Robux Packages (warehouse.dim_packages)
CREATE TABLE IF NOT EXISTS warehouse.dim_packages (
    package_id BIGINT PRIMARY KEY,
    package_name VARCHAR(255) NOT NULL,
    robux_amount INTEGER NOT NULL,
    price_idr DECIMAL(12, 2) NOT NULL,
    price_per_robux_idr DECIMAL(10, 4) NOT NULL,
    package_tier VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Dimension: Payment Methods (warehouse.dim_payment_methods)
CREATE TABLE IF NOT EXISTS warehouse.dim_payment_methods (
    payment_method_key VARCHAR(50) PRIMARY KEY,
    payment_method_name VARCHAR(100) NOT NULL,
    payment_category VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Fact Table: Robux Transactions (warehouse.fact_transactions)
CREATE TABLE IF NOT EXISTS warehouse.fact_transactions (
    transaction_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES warehouse.dim_users(user_id),
    package_id BIGINT NOT NULL REFERENCES warehouse.dim_packages(package_id),
    payment_method_key VARCHAR(50) NOT NULL REFERENCES warehouse.dim_payment_methods(payment_method_key),
    date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    roblox_id VARCHAR(100) NOT NULL,
    roblox_username VARCHAR(255),
    robux_amount INTEGER NOT NULL,
    gross_amount_idr DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    is_success BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_tx_date_key ON warehouse.fact_transactions(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_tx_status ON warehouse.fact_transactions(status);
CREATE INDEX IF NOT EXISTS idx_fact_tx_package ON warehouse.fact_transactions(package_id);

-- 6. Fact Table: User Interaction & Security Logs (warehouse.fact_interaction_logs)
CREATE TABLE IF NOT EXISTS warehouse.fact_interaction_logs (
    log_id BIGINT PRIMARY KEY,
    user_id BIGINT REFERENCES warehouse.dim_users(user_id),
    date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    action_type VARCHAR(100) NOT NULL,
    page_url TEXT,
    ip_address VARCHAR(64),
    browser VARCHAR(64),
    os VARCHAR(64),
    detail_summary TEXT,
    is_security_or_error BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_logs_date_key ON warehouse.fact_interaction_logs(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_logs_action ON warehouse.fact_interaction_logs(action_type);
