-- Privacy-First User Analytics - Database Schema
-- Three privacy zones: RAW (encrypted PII), ANONYMIZED (safe for analytics), ANALYTICS (aggregated)

-- ============================================================================
-- PRIVACY ZONE 1: RAW DATA (Restricted Access)
-- Contains original PII - encrypted at rest, limited access
-- ============================================================================

-- Raw user data (PII preserved for compliance)
CREATE TABLE IF NOT EXISTS raw_users (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    gender CHAR(1),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    ip_address INET,
    phone VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_login TIMESTAMP WITH TIME ZONE,
    account_status VARCHAR(20) NOT NULL,
    subscription_tier VARCHAR(20) NOT NULL,
    lifetime_value DECIMAL(10,2),
    total_purchases INTEGER,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_raw_user_id UNIQUE(user_id)
);

-- Raw event data (PII preserved)
CREATE TABLE IF NOT EXISTS raw_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    page_url VARCHAR(500),
    referrer VARCHAR(500),
    device_type VARCHAR(50),
    browser VARCHAR(50),
    session_id UUID,
    ip_address INET,
    search_query VARCHAR(255),
    product_id VARCHAR(50),
    quantity INTEGER,
    price DECIMAL(10,2),
    video_id VARCHAR(50),
    duration_seconds INTEGER,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_raw_event_id UNIQUE(event_id)
);

-- Indexes for raw tables (access control required)
CREATE INDEX idx_raw_users_user_id ON raw_users(user_id);
CREATE INDEX idx_raw_users_email ON raw_users(email);
CREATE INDEX idx_raw_events_user_id ON raw_events(user_id);
CREATE INDEX idx_raw_events_timestamp ON raw_events(timestamp);

-- ============================================================================
-- PRIVACY ZONE 2: ANONYMIZED DATA (Analytics Access)
-- PII removed/masked - safe for data scientists and analysts
-- ============================================================================

-- Anonymized user data (PII replaced with hashes/tokens)
CREATE TABLE IF NOT EXISTS anonymized_users (
    id SERIAL PRIMARY KEY,
    user_token VARCHAR(100) NOT NULL,  -- Tokenized user_id
    email_hash VARCHAR(64) NOT NULL,   -- Hashed email
    first_name_hash VARCHAR(64),       -- Hashed first name
    last_name_hash VARCHAR(64),        -- Hashed last name
    age_bucket VARCHAR(10),            -- Age generalized to buckets
    gender CHAR(1),
    region VARCHAR(50),                -- City generalized to region
    state VARCHAR(50),
    zip_prefix VARCHAR(5),             -- ZIP generalized (3 digits)
    ip_subnet VARCHAR(20),             -- IP masked to subnet
    phone_suffix VARCHAR(10),          -- Phone masked (last 4 digits)
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_login TIMESTAMP WITH TIME ZONE,
    account_status VARCHAR(20) NOT NULL,
    subscription_tier VARCHAR(20) NOT NULL,
    lifetime_value DECIMAL(10,2),
    total_purchases INTEGER,
    anonymized_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_anon_user_token UNIQUE(user_token)
);

-- Anonymized event data
CREATE TABLE IF NOT EXISTS anonymized_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50) NOT NULL,
    user_token VARCHAR(100) NOT NULL,  -- Must match anonymized_users
    event_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    page_url VARCHAR(500),
    referrer VARCHAR(500),
    device_type VARCHAR(50),
    browser VARCHAR(50),
    session_hash VARCHAR(64),          -- Hashed session_id
    ip_subnet VARCHAR(20),             -- Masked IP
    search_query VARCHAR(255),
    product_id VARCHAR(50),
    quantity INTEGER,
    price DECIMAL(10,2),
    video_id VARCHAR(50),
    duration_seconds INTEGER,
    anonymized_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_anon_event_id UNIQUE(event_id)
);

-- Indexes for anonymized tables (high-performance analytics)
CREATE INDEX idx_anon_users_user_token ON anonymized_users(user_token);
CREATE INDEX idx_anon_users_region ON anonymized_users(region);
CREATE INDEX idx_anon_users_age_bucket ON anonymized_users(age_bucket);
CREATE INDEX idx_anon_users_subscription ON anonymized_users(subscription_tier);
CREATE INDEX idx_anon_events_user_token ON anonymized_events(user_token);
CREATE INDEX idx_anon_events_timestamp ON anonymized_events(timestamp);
CREATE INDEX idx_anon_events_event_type ON anonymized_events(event_type);

-- ============================================================================
-- PRIVACY ZONE 3: ANALYTICS (Aggregated Metrics)
-- Pre-computed aggregations - no individual-level data
-- ============================================================================

-- User cohort analysis
CREATE TABLE IF NOT EXISTS user_cohorts (
    id SERIAL PRIMARY KEY,
    cohort_month DATE NOT NULL,
    region VARCHAR(50),
    age_bucket VARCHAR(10),
    gender CHAR(1),
    subscription_tier VARCHAR(20),
    total_users INTEGER NOT NULL,
    active_users INTEGER,
    avg_lifetime_value DECIMAL(10,2),
    avg_purchases DECIMAL(10,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Event aggregations by day
CREATE TABLE IF NOT EXISTS daily_event_metrics (
    id SERIAL PRIMARY KEY,
    event_date DATE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    region VARCHAR(50),
    device_type VARCHAR(50),
    total_events BIGINT NOT NULL,
    unique_users INTEGER NOT NULL,
    avg_session_duration INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_daily_metric UNIQUE(event_date, event_type, region, device_type)
);

-- Product analytics
CREATE TABLE IF NOT EXISTS product_metrics (
    id SERIAL PRIMARY KEY,
    analysis_date DATE NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    total_views INTEGER,
    total_add_to_cart INTEGER,
    total_purchases INTEGER,
    conversion_rate DECIMAL(5,2),
    total_revenue DECIMAL(12,2),
    avg_price DECIMAL(10,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_product_metric UNIQUE(analysis_date, product_id)
);

-- Indexes for analytics tables
CREATE INDEX idx_cohorts_month ON user_cohorts(cohort_month);
CREATE INDEX idx_daily_metrics_date ON daily_event_metrics(event_date);
CREATE INDEX idx_product_metrics_date ON product_metrics(analysis_date);

-- ============================================================================
-- METADATA & AUDIT TABLES
-- ============================================================================

-- Privacy audit log
CREATE TABLE IF NOT EXISTS privacy_audit_log (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    user_id VARCHAR(100),
    table_name VARCHAR(100),
    operation VARCHAR(50),
    records_affected INTEGER,
    performed_by VARCHAR(100),
    ip_address INET,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

CREATE INDEX idx_audit_timestamp ON privacy_audit_log(timestamp DESC);
CREATE INDEX idx_audit_event_type ON privacy_audit_log(event_type);

-- Data lineage tracking
CREATE TABLE IF NOT EXISTS data_lineage (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    transformation_type VARCHAR(50) NOT NULL,
    records_processed BIGINT,
    processing_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    spark_job_id VARCHAR(100),
    status VARCHAR(20),
    details JSONB
);

CREATE INDEX idx_lineage_timestamp ON data_lineage(processing_timestamp DESC);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- User overview (anonymized)
CREATE OR REPLACE VIEW v_user_overview AS
SELECT 
    user_token,
    age_bucket,
    gender,
    region,
    state,
    account_status,
    subscription_tier,
    lifetime_value,
    total_purchases,
    DATE_PART('day', NOW() - created_at) as account_age_days,
    DATE_PART('day', NOW() - last_login) as days_since_login
FROM anonymized_users
WHERE account_status = 'active';

-- Event summary (last 7 days)
CREATE OR REPLACE VIEW v_recent_events AS
SELECT 
    DATE_TRUNC('day', timestamp) as event_day,
    event_type,
    device_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_token) as unique_users
FROM anonymized_events
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', timestamp), event_type, device_type
ORDER BY event_day DESC, event_count DESC;

-- Subscription tier metrics
CREATE OR REPLACE VIEW v_subscription_metrics AS
SELECT 
    subscription_tier,
    region,
    COUNT(*) as user_count,
    AVG(lifetime_value) as avg_ltv,
    AVG(total_purchases) as avg_purchases,
    SUM(lifetime_value) as total_revenue
FROM anonymized_users
WHERE account_status = 'active'
GROUP BY subscription_tier, region
ORDER BY total_revenue DESC;

-- ============================================================================
-- COMMENTS (Documentation)
-- ============================================================================

COMMENT ON TABLE raw_users IS 'Raw user data with PII - RESTRICTED ACCESS - encryption required';
COMMENT ON TABLE raw_events IS 'Raw event data with PII - RESTRICTED ACCESS';
COMMENT ON TABLE anonymized_users IS 'Anonymized user data - safe for analytics';
COMMENT ON TABLE anonymized_events IS 'Anonymized event data - safe for analytics';
COMMENT ON TABLE user_cohorts IS 'Aggregated cohort metrics - no individual data';
COMMENT ON TABLE privacy_audit_log IS 'Audit trail for all privacy-sensitive operations';
COMMENT ON TABLE data_lineage IS 'Tracks data transformations and Spark job execution';

COMMENT ON COLUMN raw_users.email IS 'PII - Email address - encrypted at rest';
COMMENT ON COLUMN raw_users.ip_address IS 'PII - IP address - encrypted at rest';
COMMENT ON COLUMN anonymized_users.user_token IS 'Tokenized user_id - reversible with token map';
COMMENT ON COLUMN anonymized_users.email_hash IS 'Hashed email - irreversible';
COMMENT ON COLUMN anonymized_users.region IS 'City generalized to region for k-anonymity';

