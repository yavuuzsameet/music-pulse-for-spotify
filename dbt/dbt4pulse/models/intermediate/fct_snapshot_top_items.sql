{{ config( materialized='table' ) }}

-- This model is used to create a snapshot of the top items in Spotify
-- It is based on the top tracks and top artists data from Spotify as well as dimensions created
-- The model aggregates the data to get the top items for each snapshot date

WITH stg_tracks AS (
    SELECT * FROM {{ ref('stg_top_tracks') }}
),
dim_artists AS (
    SELECT * FROM {{ ref('dim_artists') }}
),
dim_albums AS (
    SELECT * FROM {{ ref('dim_albums') }}
)

SELECT
    -- Core Keys & Date
    stg_tracks.track_snapshot_date,
    stg_tracks.track_id,
    stg_tracks.primary_artist_id AS artist_id, -- Use primary_artist_id as the main artist link
    stg_tracks.album_id,

    -- Track Details
    stg_tracks.track_name,
    stg_tracks.track_popularity,
    stg_tracks.duration_ms,
    stg_tracks.explicit,
    stg_tracks.track_uri,

    -- Album Details (Joined)
    dim_albums.album_name,
    dim_albums.album_release_date_parsed, -- Use the parsed date from dim_albums
    dim_albums.album_type,

    -- Artist Details (Joined & enriched)
    dim_artists.artist_name,
    dim_artists.artist_popularity,
    dim_artists.artist_genres, 
    dim_artists.artist_uri,
    dim_artists.artist_image_url

FROM stg_tracks
LEFT JOIN dim_albums
    ON stg_tracks.album_id = dim_albums.album_id
LEFT JOIN dim_artists
    ON stg_tracks.primary_artist_id = dim_artists.artist_id

