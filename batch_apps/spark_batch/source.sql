-- ============================================================================
-- Synapse SQL Source Query
-- ============================================================================
-- File: source.sql
-- Purpose: Extract customer risk trigger data from Synapse source tables
-- Run Date: Configured to pull data from the logical date parameter

SELECT
    c.customer_lpid AS customerLpid,
    c.card_token AS custcardTokens,
    rt.risk_score AS riskScore,
    rt.risk_trigger_date AS riskTriggerDate,
    CASE
        WHEN rt.risk_score >= 75 THEN 'HIGH'
        WHEN rt.risk_score >= 50 THEN 'MEDIUM'
        WHEN rt.risk_score >= 25 THEN 'LOW'
        ELSE 'UNKNOWN'
    END AS riskIndicator,
    t.transaction_amount AS transactionAmount,
    CAST(t.transaction_date AS DATE) AS transactionDate,
    m.merchant_category AS merchantCategory,
    GETDATE() AS createdAt
FROM
    dbo.customer c
    INNER JOIN dbo.risk_triggers rt ON c.customer_id = rt.customer_id
    INNER JOIN dbo.transactions t ON rt.transaction_id = t.transaction_id
    LEFT JOIN dbo.merchants m ON t.merchant_id = m.merchant_id
WHERE
    -- Filter by logical date (passed from pipeline)
    CAST(rt.risk_trigger_date AS DATE) = CAST(GETDATE() AS DATE)
    -- Only active customers
    AND c.is_active = 1
    -- Only risk scores above threshold
    AND rt.risk_score > 10
    -- Exclude cancelled transactions
    AND t.transaction_status NOT IN ('CANCELLED', 'FAILED')
ORDER BY
    rt.risk_score DESC,
    rt.risk_trigger_date DESC,
    c.customer_lpid
OFFSET 0 ROWS FETCH NEXT 1000000 ROWS ONLY;

