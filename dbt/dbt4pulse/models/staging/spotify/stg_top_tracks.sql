{{ config(materialized='view') }} 

WITH source_data AS (
    -- Select from the source defined in src_spotify.yml
    -- This reads directly from the external table pointing to GCS JSON files
    SELECT *
    FROM {{ source('spotify_raw', 'raw_spotify_top_tracks') }}
)
SELECT
    -- Identifiers
    id AS track_id,
    name AS track_name,
    artists[SAFE_OFFSET(0)].id AS primary_artist_id, 
    album.id AS album_id,

    -- Track Info
    CAST(popularity as INTEGER) AS track_popularity,
    duration_ms,
    explicit,
    uri AS track_uri,

    -- Artist Info (Primary Artist)
    artists[SAFE_OFFSET(0)].name AS primary_artist_name,
    artists[SAFE_OFFSET(0)].uri AS primary_artist_uri,

    -- Album Info
    album.name AS album_name,
    album.release_date AS album_release_date,
    album.release_date_precision AS album_release_date_precision,
    album.album_type AS album_type,
    album.uri AS album_uri,
    album.images[SAFE_OFFSET(0)].url AS album_image_url,

    -- Snapshot Date
    year AS snapshot_year,
    month AS snapshot_month,
    day AS snapshot_day,
    -- snapshot_date: DATE(year, month, day) AS snapshot_date
    -- created from the partition keys
    DATE(CAST(year AS INT64), CAST(month AS INT64), CAST(day AS INT64)) AS track_snapshot_date

FROM source_data