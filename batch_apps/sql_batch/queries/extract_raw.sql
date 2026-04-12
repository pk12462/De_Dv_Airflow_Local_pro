-- =============================================================================
-- Extract Raw Events
-- =============================================================================
-- Description : Pulls raw, unprocessed records from the source table.
--               Filtered by the logical_date (Jinja2 templated).
-- Parameters  :
--   {{ logical_date }} - The batch processing date (YYYY-MM-DD)
--   {{ schema }}       - Target schema (default: public)
-- =============================================================================

INSERT INTO {{ schema }}.staging_events (
    event_id,
    event_type,
    source_system,
    event_timestamp,
    raw_payload,
    extracted_at
)
SELECT
    e.event_id,
    e.event_type,
    e.source_system,
    e.event_timestamp,
    e.raw_payload,
    CURRENT_TIMESTAMP AS extracted_at
FROM {{ schema }}.raw_events e
WHERE
    e.event_date = '{{ logical_date }}'::DATE
    AND e.is_processed = FALSE
    AND e.event_id NOT IN (
        SELECT event_id FROM {{ schema }}.staging_events
        WHERE event_date = '{{ logical_date }}'::DATE
    )
ON CONFLICT (event_id) DO NOTHING;

-- Log extraction stats
INSERT INTO {{ schema }}.pipeline_audit_log (
    pipeline_name,
    step,
    batch_date,
    record_count,
    executed_at
)
SELECT
    'sql_batch_pipeline',
    'extract',
    '{{ logical_date }}'::DATE,
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ schema }}.staging_events
WHERE event_date = '{{ logical_date }}'::DATE;

