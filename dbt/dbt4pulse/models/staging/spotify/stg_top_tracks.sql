{{ config(materialized='view') }} 

WITH source_data AS (
    -- Select from the source defined in src_spotify.yml
    -- This reads directly from the external table pointing to GCS JSON files
    SELECT *
    FROM {{ source('spotify_raw', 'raw_spotify_top_tracks') }}
)
SELECT
    -- Note: Adjust these field references if BigQuery auto-detect named them differently!
    -- Especially nested fields like album.name or artists array access. Check BQ schema first.

    -- Identifiers
    id AS track_id,
    name AS track_name,
    artists[SAFE_OFFSET(0)].id AS primary_artist_id, -- Get the first artist's ID
    album.id AS album_id,

    -- Track Info
    popularity,
    duration_ms,
    explicit,
    external_urls.spotify AS track_spotify_url,

    -- Artist Info (Primary Artist)
    artists[SAFE_OFFSET(0)].name AS primary_artist_name,

    -- Album Info
    album.name AS album_name,
    album.release_date AS album_release_date,
    album.release_date_precision AS album_release_date_precision,
    album.album_type,

    -- Partition columns (assuming BigQuery detected these from the path)
    -- Check the exact names in the BQ schema! They might be different.
    year AS snapshot_year,
    month AS snapshot_month,
    day AS snapshot_day
    -- Potentially construct a snapshot_date: DATE(year, month, day) AS snapshot_date

    -- To select ALL raw columns detected by BigQuery, you could use:
    -- * EXCEPT (year, month, day), -- Exclude partition keys if needed/desired
    -- year AS snapshot_year,
    -- month AS snapshot_month,
    -- day AS snapshot_day

FROM source_data