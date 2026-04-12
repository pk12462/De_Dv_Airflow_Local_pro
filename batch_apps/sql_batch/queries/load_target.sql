-- =============================================================================
-- Load Transformed Events → Target Table (Merge / Upsert)
-- =============================================================================
-- Description : Merges transformed events into the final target table
--               using an UPSERT pattern (INSERT ... ON CONFLICT DO UPDATE).
-- Parameters  :
--   {{ logical_date }}   - The batch processing date (YYYY-MM-DD)
--   {{ schema }}         - Target schema (default: public)
--   {{ target_table }}   - Target table name (default: processed_events)
-- =============================================================================

-- Upsert into target table
INSERT INTO {{ schema }}.{{ target_table }} (
    event_id,
    event_type,
    event_category,
    source_system,
    event_timestamp,
    event_date,
    payload_json,
    is_pii,
    batch_date,
    loaded_at
)
SELECT
    t.event_id,
    t.event_type,
    t.event_category,
    t.source_system,
    t.event_timestamp,
    t.event_date,
    t.payload_json,
    t.is_pii,
    t.batch_date,
    CURRENT_TIMESTAMP AS loaded_at
FROM {{ schema }}.transformed_events t
WHERE
    t.batch_date = '{{ logical_date }}'::DATE
ON CONFLICT (event_id) DO UPDATE
    SET
        event_type     = EXCLUDED.event_type,
        event_category = EXCLUDED.event_category,
        payload_json   = EXCLUDED.payload_json,
        is_pii         = EXCLUDED.is_pii,
        batch_date     = EXCLUDED.batch_date,
        loaded_at      = EXCLUDED.loaded_at;

-- Log load stats
INSERT INTO {{ schema }}.pipeline_audit_log (
    pipeline_name,
    step,
    batch_date,
    record_count,
    executed_at
)
SELECT
    'sql_batch_pipeline',
    'load',
    '{{ logical_date }}'::DATE,
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ schema }}.{{ target_table }}
WHERE batch_date = '{{ logical_date }}'::DATE;

-- Vacuum staging (optional — remove processed records older than 7 days)
DELETE FROM {{ schema }}.staging_events
WHERE
    event_date < ('{{ logical_date }}'::DATE - INTERVAL '7 days')
    AND is_processed = TRUE;

