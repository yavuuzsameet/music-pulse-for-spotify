{{ config(materialized='view') }} -- Keep as view for latest snapshot persistence

WITH fact_data AS (
    -- Select distinct track appearances per snapshot date
    SELECT DISTINCT
        track_snapshot_date,
        track_id,
        track_name
    FROM {{ ref('fct_snapshot_top_items') }}
    WHERE track_id IS NOT NULL
),

ranked_snapshots AS (
    -- Assign two ranks:
    -- 1. A rank based on the overall order of snapshot dates
    -- 2. A rank based on the order of appearance for each specific track
    SELECT
        track_snapshot_date,
        track_id,
        track_name,
        DENSE_RANK() OVER (ORDER BY track_snapshot_date ASC) AS snapshot_rank,
        ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY track_snapshot_date ASC) AS track_appearance_rank
        -- Using DENSE_RANK for snapshot_date handles cases where multiple runs might happen on the same day
        -- Using ROW_NUMBER for track ensures unique rank within track's appearances
    FROM fact_data
),

streak_identifier AS (
    -- Identify groups of consecutive snapshots for each track.
    -- The difference between the overall snapshot rank and the track's appearance rank
    -- will be constant for records within the same consecutive streak.
    SELECT
        *,
        (snapshot_rank - track_appearance_rank) AS streak_group_id
    FROM ranked_snapshots
),

streak_calculation AS (
    -- Calculate the length of each streak (group) for each track
    SELECT
        track_id,
        track_name,
        streak_group_id,
        COUNT(*) AS consecutive_snapshots, -- Count rows within the streak group
        MAX(track_snapshot_date) AS streak_end_date -- The end date of this specific streak
    FROM streak_identifier
    GROUP BY
        track_id,
        track_name,
        streak_group_id
)
-- Final Selection: Only show the persistence for tracks present in the LATEST snapshot
SELECT
    s.track_id,
    s.track_name,
    s.consecutive_snapshots AS consecutive_snapshots_in_top_10
FROM streak_calculation s
WHERE s.streak_end_date = (SELECT MAX(track_snapshot_date) FROM fact_data) -- Filter for streaks ending on the most recent date
ORDER BY
    consecutive_snapshots_in_top_10 DESC,
    s.track_name