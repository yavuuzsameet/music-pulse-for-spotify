{{ config(
    materialized='incremental',
    unique_key='artist_id',
    ) 
}}

WITH latest_artist_snapshot AS (
    -- Find the latest snapshot record for each artist to get their most recent details
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY artist_snapshot_date DESC) as rn
    FROM {{ ref('stg_top_artists') }}
    WHERE artist_id IS NOT NULL 
)
SELECT
    artist_id,
    artist_name,
    artist_popularity,
    artist_genres,       
    artist_uri,
    artist_image_url,
    artist_snapshot_date AS last_seen_artist_snapshot_date
FROM latest_artist_snapshot
WHERE rn = 1 