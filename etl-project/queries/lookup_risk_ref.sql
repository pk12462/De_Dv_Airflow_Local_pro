-- Lookup query: fetch user tier / region reference data
-- Used by the LOOKUP transformation step to enrich the main dataset.

SELECT
    user_id,
    tier,
    region
FROM
    user_tier_reference
WHERE
    active = TRUE;

