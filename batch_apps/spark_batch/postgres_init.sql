-- ============================================================================
-- PostgreSQL Initialization Script
-- ============================================================================
-- This script is executed after postgres_schema.sql
-- It populates initial data for testing/development

-- Insert sample data for testing
INSERT INTO cust_risk_triggers (
    customerLpid, custcardTokens, riskScore, riskTriggerDate,
    riskIndicator, transactionAmount, transactionDate,
    merchantCategory, createdAt, processedAt,
    processDate, pipelineVersion, dataQuality
) VALUES
('LPID001', 'TOKEN_4523xxxx', 85.50, '2026-04-11 10:30:00', 'HIGH', 5000.00, '2026-04-11', 'RETAIL', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID002', 'TOKEN_7891xxxx', 45.25, '2026-04-11 11:15:00', 'MEDIUM', 1500.00, '2026-04-11', 'GROCERY', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID003', 'TOKEN_5634xxxx', 92.75, '2026-04-11 09:45:00', 'HIGH', 8500.00, '2026-04-11', 'TRAVEL', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID004', 'TOKEN_2109xxxx', 15.50, '2026-04-11 14:20:00', 'LOW', 300.00, '2026-04-11', 'DINING', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID005', 'TOKEN_8765xxxx', 55.30, '2026-04-11 12:30:00', 'MEDIUM', 3200.00, '2026-04-11', 'UTILITIES', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID006', 'TOKEN_3456xxxx', 78.90, '2026-04-11 13:45:00', 'HIGH', 6500.00, '2026-04-11', 'JEWELRY', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID007', 'TOKEN_1234xxxx', 22.10, '2026-04-11 15:10:00', 'LOW', 450.00, '2026-04-11', 'ENTERTAINMENT', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID008', 'TOKEN_9876xxxx', 68.50, '2026-04-11 16:25:00', 'MEDIUM', 4200.00, '2026-04-11', 'PHARMACIES', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID009', 'TOKEN_5555xxxx', 88.75, '2026-04-11 10:00:00', 'HIGH', 7500.00, '2026-04-11', 'AUTOMOTIVE', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID010', 'TOKEN_4444xxxx', 35.20, '2026-04-11 17:40:00', 'LOW', 600.00, '2026-04-11', 'BOOKS', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID011', 'TOKEN_3333xxxx', 72.45, '2026-04-11 11:50:00', 'MEDIUM', 5500.00, '2026-04-11', 'SPORTS', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID012', 'TOKEN_2222xxxx', 95.80, '2026-04-11 08:15:00', 'HIGH', 20000.00, '2026-04-11', 'LUXURY', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID013', 'TOKEN_1111xxxx', 12.30, '2026-04-11 18:30:00', 'UNKNOWN', 250.00, '2026-04-11', 'MISC', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID014', 'TOKEN_9999xxxx', 58.70, '2026-04-11 12:00:00', 'MEDIUM', 2800.00, '2026-04-11', 'INSURANCE', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID015', 'TOKEN_8888xxxx', 82.15, '2026-04-11 09:30:00', 'HIGH', 9200.00, '2026-04-11', 'HEALTHCARE', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID016', 'TOKEN_7777xxxx', 48.60, '2026-04-11 14:45:00', 'MEDIUM', 1800.00, '2026-04-11', 'EDUCATION', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID017', 'TOKEN_6666xxxx', 76.40, '2026-04-11 11:20:00', 'HIGH', 7000.00, '2026-04-11', 'HOSPITALITY', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID018', 'TOKEN_5555xxxx', 31.25, '2026-04-11 15:55:00', 'LOW', 550.00, '2026-04-11', 'TELECOMMUNICATIONS', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID019', 'TOKEN_4444xxxx', 64.80, '2026-04-11 13:10:00', 'MEDIUM', 4500.00, '2026-04-11', 'TRANSPORTATION', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID020', 'TOKEN_3333xxxx', 89.50, '2026-04-11 10:40:00', 'HIGH', 15000.00, '2026-04-11', 'REAL_ESTATE', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID021', 'TOKEN_2222xxxx', 28.90, '2026-04-11 16:00:00', 'LOW', 400.00, '2026-04-11', 'GAMING', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID022', 'TOKEN_1111xxxx', 71.35, '2026-04-11 12:25:00', 'MEDIUM', 3500.00, '2026-04-11', 'FASHION', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID023', 'TOKEN_0000xxxx', 86.20, '2026-04-11 09:50:00', 'HIGH', 6800.00, '2026-04-11', 'CONSUMER_GOODS', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID024', 'TOKEN_xxxxxx01', 42.15, '2026-04-11 14:30:00', 'MEDIUM', 1200.00, '2026-04-11', 'SUBSCRIPTION', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID025', 'TOKEN_xxxxxx02', 77.65, '2026-04-11 11:05:00', 'HIGH', 8200.00, '2026-04-11', 'FINANCIAL_SERVICES', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID026', 'TOKEN_xxxxxx03', 25.45, '2026-04-11 17:15:00', 'LOW', 500.00, '2026-04-11', 'UTILITIES', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID027', 'TOKEN_xxxxxx04', 59.80, '2026-04-11 13:35:00', 'MEDIUM', 3800.00, '2026-04-11', 'RETAIL', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID028', 'TOKEN_xxxxxx05', 91.10, '2026-04-11 10:20:00', 'HIGH', 12000.00, '2026-04-11', 'COMMODITIES', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID029', 'TOKEN_xxxxxx06', 38.70, '2026-04-11 15:45:00', 'LOW', 700.00, '2026-04-11', 'ENERGY', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED'),
('LPID030', 'TOKEN_xxxxxx07', 67.30, '2026-04-11 12:50:00', 'MEDIUM', 4000.00, '2026-04-11', 'CHEMICALS', NOW(), NOW(), CURRENT_DATE, '1.0.0', 'VALIDATED');

-- Insert initial data quality log entry
INSERT INTO data_quality_log (
    pipelineVersion, processDate, totalRecords, validRecords,
    invalidRecords, duplicateRecords, nullRecords,
    qualityScore, processingStartTime, processingEndTime,
    processingDuration, status, createdAt
) VALUES
('1.0.0', CURRENT_DATE, 30, 30, 0, 0, 0, 100.00, NOW() - INTERVAL '5 minutes', NOW(), INTERVAL '5 minutes', 'SUCCESS', NOW());

-- Insert initial pipeline execution log
INSERT INTO pipeline_execution_log (
    appName, dataSetName, executionDate, executionStartTime,
    executionEndTime, executionDuration, sourceName, targetName,
    recordsProcessed, recordsLoaded, status, createdAt
) VALUES
('btot-8999-syn-api-cust-risk-tg', 'cust-risk-triggers', CURRENT_DATE, NOW() - INTERVAL '5 minutes',
 NOW(), INTERVAL '5 minutes', 'Synapse', 'PostgreSQL+API', 30, 30, 'SUCCESS', NOW());

-- Create materialized view for better query performance
CREATE MATERIALIZED VIEW mv_customer_risk_profile AS
SELECT
    customerLpid,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT transactionDate) as transaction_days,
    AVG(riskScore) as avg_risk_score,
    MAX(riskScore) as max_risk_score,
    MIN(riskScore) as min_risk_score,
    SUM(transactionAmount) as total_amount,
    AVG(transactionAmount) as avg_amount,
    COUNT(DISTINCT merchantCategory) as merchant_categories,
    MAX(transactionDate) as last_transaction_date,
    MAX(processedAt) as last_processed_at
FROM cust_risk_triggers
WHERE processDate >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY customerLpid;

-- Create index on materialized view
CREATE INDEX idx_mv_customer_risk_profile_lpid ON mv_customer_risk_profile(customerLpid);

-- Log successful initialization
INSERT INTO pipeline_execution_log (
    appName, dataSetName, executionDate, executionStartTime,
    executionEndTime, sourceName, targetName,
    recordsProcessed, recordsLoaded, status, errorMessage, createdAt
) VALUES
('postgres-initialization', 'schema-setup', CURRENT_DATE, NOW(), NOW(),
 'PostgreSQL', 'localhost', 30, 30, 'SUCCESS', NULL, NOW());

