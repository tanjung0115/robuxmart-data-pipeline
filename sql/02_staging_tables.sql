-- ============================================================================
-- 02_staging_tables.sql
-- Staging tables for landing cleaned data and tracking ETL pipeline runs
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.etl_run_history (
    run_id VARCHAR(64) PRIMARY KEY,
    source_mode VARCHAR(32) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    rows_extracted INTEGER DEFAULT 0,
    rows_cleaned INTEGER DEFAULT 0,
    rows_quarantined INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_users (
    user_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    username VARCHAR(255),
    email_masked VARCHAR(255) NOT NULL,
    email_domain VARCHAR(120) NOT NULL,
    phone_masked VARCHAR(64),
    is_admin BOOLEAN DEFAULT FALSE,
    is_verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_packages (
    package_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    robux_amount INTEGER NOT NULL,
    price DECIMAL(12, 2) NOT NULL,
    price_per_robux DECIMAL(10, 4) NOT NULL,
    package_tier VARCHAR(50) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_transactions (
    transaction_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    package_id BIGINT NOT NULL,
    package_name VARCHAR(255) NOT NULL,
    robux_amount INTEGER NOT NULL,
    price DECIMAL(12, 2) NOT NULL,
    roblox_id VARCHAR(100) NOT NULL,
    roblox_username VARCHAR(255),
    payment_method VARCHAR(50) NOT NULL,
    payment_category VARCHAR(50) NOT NULL,
    payment_token VARCHAR(255),
    status VARCHAR(20) NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_interaction_logs (
    log_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    action VARCHAR(100) NOT NULL,
    ip_address VARCHAR(64),
    browser VARCHAR(64),
    os VARCHAR(64),
    page_url TEXT,
    detail_summary TEXT,
    is_security_or_error BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
