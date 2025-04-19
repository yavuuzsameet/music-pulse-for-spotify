{{ config( materialized='table' ) }}

-- This model is used to create a snapshot of the top items in Spotify
-- It is based on the top tracks and top artists data from Spotify
-- The model aggregates the data to get the top items for each snapshot date

WITH 
stg_tracks AS (
    SELECT *
    FROM {{ ref('stg_top_tracks') }}
),
stg_artists AS (
    SELECT 
        artist_id,
        artist_name,
        artist_popularity,
        artist_genres,
        artist_image_url,
        artist_uri,
        artist_snapshot_date
    FROM {{ ref('stg_top_artists') }}
)

SELECT
    -- Track details from stg_tracks
    stg_tracks.track_snapshot_date,
    stg_tracks.track_id,
    stg_tracks.track_name,
    stg_tracks.track_popularity,
    stg_tracks.track_uri,
    stg_tracks.duration_ms,

    -- Album details from stg_tracks
    stg_tracks.album_id,
    stg_tracks.album_name,
    stg_tracks.album_uri,
    stg_tracks.album_image_url,

    stg_tracks.album_release_date,
    stg_tracks.album_release_date_precision,

    CASE stg_tracks.album_release_date_precision
        WHEN 'day' THEN SAFE.PARSE_DATE('%Y-%m-%d', stg_tracks.album_release_date)
        WHEN 'month' THEN SAFE.PARSE_DATE('%Y-%m', stg_tracks.album_release_date)
        WHEN 'year' THEN SAFE.PARSE_DATE('%Y', stg_tracks.album_release_date)
        ELSE NULL
    END AS album_release_date_parsed,

    -- Artist details from stg_artists
    stg_artists.artist_id,
    stg_artists.artist_name,
    stg_artists.artist_popularity,
    stg_artists.artist_genres,
    stg_artists.artist_image_url,
    stg_artists.artist_uri,

FROM stg_tracks
LEFT JOIN stg_artists
    ON stg_tracks.primary_artist_id = stg_artists.artist_id
    AND stg_tracks.track_snapshot_date = stg_artists.artist_snapshot_date


