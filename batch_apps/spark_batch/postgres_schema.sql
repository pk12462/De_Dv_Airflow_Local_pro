 ke-- ============================================================================
-- Bank Customer Risk Triggers Database Schema
-- ============================================================================
-- Database: bank_db
-- Schema: public
-- Application: btot-8999-syn-api-cust-risk-tg
-- Tables: cust_risk_triggers, cust_risk_triggers_audit, data_quality_log

-- Set schema
SET search_path TO public;

-- ============================================================================
-- 1. Customer Risk Triggers Main Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cust_risk_triggers (
    id BIGSERIAL PRIMARY KEY,
    customerLpid VARCHAR(50) NOT NULL,
    custcardTokens VARCHAR(255),
    riskScore NUMERIC(10, 2),
    riskTriggerDate TIMESTAMP,
    riskIndicator VARCHAR(50),
    transactionAmount NUMERIC(15, 2),
    transactionDate DATE,
    merchantCategory VARCHAR(100),
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processDate DATE DEFAULT CURRENT_DATE,
    pipelineVersion VARCHAR(20),
    dataQuality VARCHAR(20),
    CONSTRAINT cust_risk_triggers_unique UNIQUE (customerLpid, transactionDate)
);

-- Create indexes for better query performance
CREATE INDEX idx_cust_risk_triggers_customerLpid ON cust_risk_triggers(customerLpid);
CREATE INDEX idx_cust_risk_triggers_transactionDate ON cust_risk_triggers(transactionDate);
CREATE INDEX idx_cust_risk_triggers_riskIndicator ON cust_risk_triggers(riskIndicator);
CREATE INDEX idx_cust_risk_triggers_processDate ON cust_risk_triggers(processDate);

-- ============================================================================
-- 2. Customer Risk Triggers Audit Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cust_risk_triggers_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    id BIGINT REFERENCES cust_risk_triggers(id) ON DELETE CASCADE,
    old_riskScore NUMERIC(10, 2),
    new_riskScore NUMERIC(10, 2),
    old_riskIndicator VARCHAR(50),
    new_riskIndicator VARCHAR(50),
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_reason VARCHAR(255)
);

CREATE INDEX idx_cust_risk_triggers_audit_id ON cust_risk_triggers_audit(id);
CREATE INDEX idx_cust_risk_triggers_audit_changed_at ON cust_risk_triggers_audit(changed_at);

-- ============================================================================
-- 3. Data Quality Log Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id BIGSERIAL PRIMARY KEY,
    pipelineVersion VARCHAR(20),
    processDate DATE,
    totalRecords BIGINT,
    validRecords BIGINT,
    invalidRecords BIGINT,
    duplicateRecords BIGINT,
    nullRecords BIGINT,
    qualityScore NUMERIC(5, 2),
    processingStartTime TIMESTAMP,
    processingEndTime TIMESTAMP,
    processingDuration INTERVAL,
    status VARCHAR(20),
    errorMessage TEXT,
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_data_quality_log_processDate ON data_quality_log(processDate);
CREATE INDEX idx_data_quality_log_status ON data_quality_log(status);

-- ============================================================================
-- 4. Pipeline Execution Log Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    execution_id BIGSERIAL PRIMARY KEY,
    appName VARCHAR(100),
    dataSetName VARCHAR(100),
    executionDate DATE,
    executionStartTime TIMESTAMP,
    executionEndTime TIMESTAMP,
    executionDuration INTERVAL,
    sourceName VARCHAR(50),
    targetName VARCHAR(50),
    recordsProcessed BIGINT,
    recordsLoaded BIGINT,
    status VARCHAR(20),
    errorMessage TEXT,
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pipeline_execution_log_executionDate ON pipeline_execution_log(executionDate);
CREATE INDEX idx_pipeline_execution_log_status ON pipeline_execution_log(status);

-- ============================================================================
-- 5. Views for common queries
-- ============================================================================

-- View: High-risk customers
CREATE OR REPLACE VIEW v_high_risk_customers AS
SELECT
    customerLpid,
    COUNT(*) as risk_count,
    AVG(riskScore) as avg_risk_score,
    MAX(riskTriggerDate) as last_risk_trigger,
    ARRAY_AGG(DISTINCT riskIndicator) as risk_indicators
FROM cust_risk_triggers
WHERE riskScore > 50 AND riskTriggerDate >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY customerLpid
HAVING COUNT(*) > 1
ORDER BY avg_risk_score DESC;

-- View: Daily risk summary
CREATE OR REPLACE VIEW v_daily_risk_summary AS
SELECT
    processDate,
    COUNT(*) as total_records,
    COUNT(DISTINCT customerLpid) as unique_customers,
    AVG(riskScore) as avg_risk_score,
    MAX(riskScore) as max_risk_score,
    MIN(riskScore) as min_risk_score,
    COUNT(CASE WHEN riskIndicator = 'HIGH' THEN 1 END) as high_risk_count,
    COUNT(CASE WHEN riskIndicator = 'MEDIUM' THEN 1 END) as medium_risk_count
FROM cust_risk_triggers
GROUP BY processDate
ORDER BY processDate DESC;

-- ============================================================================
-- 6. Functions for data validation and quality checks
-- ============================================================================

-- Function: Check data quality for a given date
CREATE OR REPLACE FUNCTION check_data_quality(p_processDate DATE)
RETURNS TABLE (
    quality_metric VARCHAR,
    metric_value NUMERIC,
    metric_status VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'Total Records'::VARCHAR, COUNT(*)::NUMERIC, 'OK'::VARCHAR
    FROM cust_risk_triggers WHERE processDate = p_processDate
    UNION ALL
    SELECT 'Duplicate Records'::VARCHAR,
           (SELECT COUNT(*) FROM (
               SELECT customerLpid, transactionDate, COUNT(*)
               FROM cust_risk_triggers
               WHERE processDate = p_processDate
               GROUP BY customerLpid, transactionDate
               HAVING COUNT(*) > 1
           ) t)::NUMERIC, 'OK'
    UNION ALL
    SELECT 'Null Risk Scores'::VARCHAR,
           COUNT(CASE WHEN riskScore IS NULL THEN 1 END)::NUMERIC, 'OK'
    FROM cust_risk_triggers WHERE processDate = p_processDate;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 7. Grant permissions (adjust based on your security requirements)
-- ============================================================================

-- Grant SELECT to read-only user
-- GRANT SELECT ON cust_risk_triggers TO readonly_user;
-- GRANT SELECT ON cust_risk_triggers_audit TO readonly_user;
-- GRANT SELECT ON data_quality_log TO readonly_user;
-- GRANT SELECT ON pipeline_execution_log TO readonly_user;

-- Grant all permissions to data pipeline user
-- GRANT ALL PRIVILEGES ON cust_risk_triggers TO pipeline_user;
-- GRANT ALL PRIVILEGES ON cust_risk_triggers_audit TO pipeline_user;
-- GRANT ALL PRIVILEGES ON data_quality_log TO pipeline_user;
-- GRANT ALL PRIVILEGES ON pipeline_execution_log TO pipeline_user;

-- ============================================================================
-- Comments for documentation
-- ============================================================================

COMMENT ON TABLE cust_risk_triggers IS 'Stores customer risk trigger events from Synapse source';
COMMENT ON COLUMN cust_risk_triggers.customerLpid IS 'Unique customer identifier (LPID)';
COMMENT ON COLUMN cust_risk_triggers.riskScore IS 'Risk score from 0-100, higher indicates more risk';
COMMENT ON COLUMN cust_risk_triggers.riskIndicator IS 'Risk level indicator: HIGH, MEDIUM, LOW, UNKNOWN';

COMMENT ON TABLE cust_risk_triggers_audit IS 'Audit trail for risk indicator changes';
COMMENT ON TABLE data_quality_log IS 'Data quality metrics and checks';
COMMENT ON TABLE pipeline_execution_log IS 'Pipeline execution history and metrics';

