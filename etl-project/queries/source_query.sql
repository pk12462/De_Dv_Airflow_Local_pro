-- Source query: read user events from the events table
-- This is the starting point of the pipeline (SOURCE step).
-- Replace the table name and columns to match your real database.

SELECT
    e.user_id,
    e.event_type,
    e.amount,
    e.event_date,
    e.status
FROM
    user_events e
WHERE
    e.event_date >= CURRENT_DATE - INTERVAL '1 day'
    AND e.status IN ('COMPLETED', 'INCOMPLETE')
ORDER BY
    e.event_date DESC;

