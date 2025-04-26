{{ config(materialized='table') }}

WITH fct_data AS (
    -- Select the necessary columns from our fact table
    SELECT
        track_snapshot_date,
        track_popularity, -- Popularity of the track itself
        artist_popularity -- Popularity of the primary artist (from dim_artists)
    FROM {{ ref('fct_snapshot_top_items') }}
)
SELECT
    track_snapshot_date,
    -- Calculate average popularity, ignoring NULLs
    AVG(track_popularity) AS avg_track_popularity,
    AVG(artist_popularity) AS avg_artist_popularity
FROM fct_data
GROUP BY
    track_snapshot_date
ORDER BY
    track_snapshot_date DESC