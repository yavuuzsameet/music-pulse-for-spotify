{{ config(materialized='table') }}

WITH fact_table AS (
    -- Select relevant columns from the core fact table
    SELECT
        track_snapshot_date,
        track_id,
        track_name, -- Keep for potential inspection/display
        artist_genres -- This is the ARRAY<STRING>
    FROM {{ ref('fct_snapshot_top_items') }}
    -- Ensure we only consider snapshots where artist data was successfully joined and genres exist
    WHERE track_id IS NOT NULL AND ARRAY_LENGTH(artist_genres) > 0
),

unnested_genres AS (
    -- Explode the genres array so each row represents one artist and one genre for a snapshot date
    -- Also, keep track of distinct artists per snapshot date here
    SELECT DISTINCT -- Ensure one row per artist per snapshot before unnesting
        track_snapshot_date,
        track_id,
        track_name,
        genre -- The individual genre string after unnesting
    FROM fact_table
    CROSS JOIN UNNEST(artist_genres) AS genre
),

genre_counts_per_snapshot AS (
    -- Count distinct artists per genre per snapshot date
    SELECT
        track_snapshot_date,
        genre,
        COUNT(DISTINCT track_id) AS track_count
    FROM unnested_genres
    GROUP BY
        track_snapshot_date,
        genre
),

total_tracks_per_snapshot AS (
     -- Calculate the total number of unique artists *with genres* for each snapshot date
    SELECT
        track_snapshot_date,
        COUNT(DISTINCT track_id) AS total_tracks
    FROM unnested_genres -- Count distinct artists AFTER unnesting/filtering
    GROUP BY
        track_snapshot_date
)
-- Final Mart Table:
SELECT
    g.track_snapshot_date,
    g.genre,
    g.track_count,
    t.total_tracks,
    -- Calculate percentage, avoiding division by zero
    SAFE_DIVIDE(g.track_count, t.total_tracks) * 100 AS percentage_of_tracks
FROM genre_counts_per_snapshot g
JOIN total_tracks_per_snapshot t
    ON g.track_snapshot_date = t.track_snapshot_date
ORDER BY
    g.track_snapshot_date DESC,
    g.track_count DESC