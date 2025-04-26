{{ config(materialized='table') }}

WITH fct_data AS (
    -- Select the necessary columns from our fact table
    SELECT
        track_snapshot_date,
        track_id,
        album_release_date_parsed -- The DATE column we created earlier
    FROM {{ ref('fct_snapshot_top_items') }}
    WHERE
        album_release_date_parsed IS NOT NULL -- Only consider tracks with a valid release date
),

track_decades AS (
    -- Assign a decade category based on the album release year
    SELECT
        track_snapshot_date,
        track_id,
        album_release_date_parsed,
        EXTRACT(YEAR FROM album_release_date_parsed) AS release_year,
        CASE
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 2020 THEN '2020s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 2010 THEN '2010s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 2000 THEN '2000s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 1990 THEN '1990s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 1980 THEN '1980s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 1970 THEN '1970s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 1960 THEN '1960s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 1950 THEN '1950s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 1940 THEN '1940s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 1930 THEN '1930s'
            WHEN EXTRACT(YEAR FROM album_release_date_parsed) >= 1920 THEN '1920s'
            ELSE 'Older or Unknown'
        END AS release_decade
    FROM fct_data
),

counts_per_decade AS (
    -- Count distinct tracks per decade per snapshot date
    SELECT
        track_snapshot_date,
        release_decade,
        COUNT(DISTINCT track_id) AS track_count
    FROM track_decades
    GROUP BY
        track_snapshot_date,
        release_decade
),

total_tracks_per_snapshot AS (
    -- Count total distinct tracks per snapshot date (for percentage calculation)
    SELECT
        track_snapshot_date,
        COUNT(DISTINCT track_id) AS total_tracks
    FROM track_decades -- Use track_decades to count only tracks included in vintage calc
    GROUP BY
        track_snapshot_date
)
-- Final Mart Table: Snapshot Date, Decade, Track Count for Decade, Total Tracks for Snapshot, Percentage
SELECT
    d.track_snapshot_date,
    d.release_decade,
    d.track_count,
    t.total_tracks,
    SAFE_DIVIDE(d.track_count, t.total_tracks) * 100 AS percentage_of_tracks
FROM counts_per_decade d
JOIN total_tracks_per_snapshot t
    ON d.track_snapshot_date = t.track_snapshot_date
ORDER BY
    d.track_snapshot_date DESC,
    -- Order decades chronologically for charting
    CASE d.release_decade
        WHEN '2020s' THEN 1
        WHEN '2010s' THEN 2
        WHEN '2000s' THEN 3
        WHEN '1990s' THEN 4
        WHEN '1980s' THEN 5
        WHEN '1970s' THEN 6
        WHEN '1960s' THEN 7
        WHEN '1950s' THEN 8
        WHEN '1940s' THEN 9
        WHEN '1930s' THEN 10
        WHEN '1920s' THEN 11
        ELSE 13
    END