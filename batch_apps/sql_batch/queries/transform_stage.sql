-- =============================================================================
-- Transform Staging Events
-- =============================================================================
-- Description : Applies cleansing, deduplication, and business rules
--               to staged raw events, populating the transform layer.
-- Parameters  :
--   {{ logical_date }} - The batch processing date (YYYY-MM-DD)
--   {{ schema }}       - Target schema (default: public)
-- =============================================================================

-- Step 1: Cleanse — remove rows with null mandatory fields
DELETE FROM {{ schema }}.staging_events
WHERE
    event_date = '{{ logical_date }}'::DATE
    AND (
        event_id   IS NULL
        OR event_type IS NULL
        OR event_timestamp IS NULL
    );

-- Step 2: Deduplicate — keep only the most recent record per event_id
DELETE FROM {{ schema }}.staging_events
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM {{ schema }}.staging_events
    WHERE event_date = '{{ logical_date }}'::DATE
    GROUP BY event_id
)
AND event_date = '{{ logical_date }}'::DATE;

-- Step 3: Apply business rules and populate transform table
INSERT INTO {{ schema }}.transformed_events (
    event_id,
    event_type,
    event_category,
    source_system,
    event_timestamp,
    event_date,
    payload_json,
    is_pii,
    batch_date,
    transformed_at
)
SELECT
    s.event_id,
    s.event_type,
    -- Business rule: categorize events
    CASE
        WHEN s.event_type IN ('login', 'logout', 'register') THEN 'auth'
        WHEN s.event_type IN ('purchase', 'refund', 'cart_add') THEN 'commerce'
        WHEN s.event_type IN ('page_view', 'click', 'search') THEN 'engagement'
        ELSE 'other'
    END                                      AS event_category,
    s.source_system,
    s.event_timestamp,
    s.event_timestamp::DATE                  AS event_date,
    s.raw_payload::JSONB                     AS payload_json,
    -- PII flag: mark events with user data
    CASE
        WHEN s.raw_payload::JSONB ? 'email'    THEN TRUE
        WHEN s.raw_payload::JSONB ? 'phone'    THEN TRUE
        WHEN s.raw_payload::JSONB ? 'ssn'      THEN TRUE
        ELSE FALSE
    END                                      AS is_pii,
    '{{ logical_date }}'::DATE               AS batch_date,
    CURRENT_TIMESTAMP                        AS transformed_at
FROM {{ schema }}.staging_events s
WHERE s.event_date = '{{ logical_date }}'::DATE
ON CONFLICT (event_id) DO UPDATE
    SET
        event_category = EXCLUDED.event_category,
        payload_json   = EXCLUDED.payload_json,
        is_pii         = EXCLUDED.is_pii,
        transformed_at = EXCLUDED.transformed_at;

-- Update staging status
UPDATE {{ schema }}.staging_events
SET is_processed = TRUE,
    processed_at = CURRENT_TIMESTAMP
WHERE event_date = '{{ logical_date }}'::DATE;

