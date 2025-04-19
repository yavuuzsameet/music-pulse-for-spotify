{{ config(
    materialized='incremental',
    unique_key='album_id'
) }}

WITH latest_album_snapshot AS (
    -- Find the latest snapshot record for each album appearing in the top tracks list
    SELECT
        album_id,                       
        album_name,
        album_release_date,
        album_release_date_precision,
        album_type,
        track_snapshot_date,                 
        -- Parse the release date based on precision
        CASE album_release_date_precision
            WHEN 'day' THEN SAFE.PARSE_DATE('%Y-%m-%d', album_release_date)
            WHEN 'month' THEN SAFE.PARSE_DATE('%Y-%m', album_release_date)
            WHEN 'year' THEN SAFE.PARSE_DATE('%Y', album_release_date)
            ELSE NULL
        END AS album_release_date_parsed,
        -- Assign row number to get the most recent record per album
        ROW_NUMBER() OVER(PARTITION BY album_id ORDER BY track_snapshot_date DESC) as rn
    FROM {{ ref('stg_top_tracks') }}
    WHERE album_id IS NOT NULL 
)
SELECT
    album_id,
    album_name,
    album_release_date_parsed,
    album_type,
    track_snapshot_date AS last_seen_album_snapshot_date 
FROM latest_album_snapshot
WHERE rn = 1 