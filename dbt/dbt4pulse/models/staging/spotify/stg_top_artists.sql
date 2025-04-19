{{ config (materialized='view') }}
WITH source_data AS (
    -- Select from the source defined in src_spotify.yml
    -- This reads directly from the external table pointing to GCS JSON files
    SELECT *
    FROM {{ source('spotify_raw', 'raw_spotify_top_artists') }}
)
SELECT
    -- Identifiers
    id AS artist_id,
    name AS artist_name,

    -- Artist Info
    uri AS artist_uri,
    CAST(popularity AS INTEGER) AS artist_popularity,
    genres AS artist_genres, 
    followers.total AS artist_follower_count,
    images[SAFE_OFFSET(0)].url AS artist_image_url,

    -- Snapshot Date
    CAST(year AS INTEGER) AS snapshot_year,
    CAST(month AS INTEGER) AS snapshot_month,
    CAST(day AS INTEGER) AS snapshot_day,
    -- Construct a snapshot date from partition keys
    DATE(CAST(year AS INT64), CAST(month AS INT64), CAST(day AS INT64)) AS artist_snapshot_date

FROM source_data