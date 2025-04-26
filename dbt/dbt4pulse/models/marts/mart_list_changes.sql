{{ config(materialized='table') }}

WITH track_snapshots AS (
    -- Get distinct snapshot dates specifically from the track staging table
    SELECT DISTINCT track_snapshot_date AS snapshot_date
    FROM {{ ref('stg_top_tracks') }}
),

artist_snapshots AS (
     -- Get distinct snapshot dates specifically from the artist staging table
    SELECT DISTINCT artist_snapshot_date AS snapshot_date
    FROM {{ ref('stg_top_artists') }}
),

snapshots AS (
    -- Combine all unique snapshot dates found in either table
    SELECT snapshot_date FROM track_snapshots
    UNION DISTINCT
    SELECT snapshot_date FROM artist_snapshots
),

snapshot_dates AS (
    -- Identify the latest snapshot date and the one immediately before it
    SELECT
        snapshot_date,
        LAG(snapshot_date, 1) OVER (ORDER BY snapshot_date ASC) as previous_snapshot_date
    FROM snapshots
),
latest_snapshots AS (
     -- Select the pair of dates we want to compare
     -- Only proceed if a previous snapshot date exists
    SELECT snapshot_date, previous_snapshot_date
    FROM snapshot_dates
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM snapshots)
    AND previous_snapshot_date IS NOT NULL
),
items_current_tracks AS (
    -- Get distinct tracks for the latest snapshot date
    SELECT DISTINCT track_id, track_name
    FROM {{ ref('stg_top_tracks') }}
    WHERE track_snapshot_date = (SELECT snapshot_date FROM latest_snapshots)
),
items_previous_tracks AS (
    -- Get distinct tracks for the previous snapshot date
    SELECT DISTINCT track_id, track_name
    FROM {{ ref('stg_top_tracks') }}
    WHERE track_snapshot_date = (SELECT previous_snapshot_date FROM latest_snapshots)
),
new_tracks AS (
    -- Tracks in current snapshot but not in previous
    SELECT track_id, track_name FROM items_current_tracks
    EXCEPT DISTINCT
    SELECT track_id, track_name FROM items_previous_tracks
),
dropped_tracks AS (
     -- Tracks in previous snapshot but not in current
     SELECT track_id, track_name FROM items_previous_tracks
     EXCEPT DISTINCT
     SELECT track_id, track_name FROM items_current_tracks
),
items_current_artists AS (
     -- Get distinct artists for the latest snapshot date
    SELECT DISTINCT artist_id, artist_name
    FROM {{ ref('stg_top_artists') }}
    WHERE artist_snapshot_date = (SELECT snapshot_date FROM latest_snapshots)
),
items_previous_artists AS (
    -- Get distinct artists for the previous snapshot date
    SELECT DISTINCT artist_id, artist_name
    FROM {{ ref('stg_top_artists') }}
    WHERE artist_snapshot_date = (SELECT previous_snapshot_date FROM latest_snapshots)
),
new_artists AS (
     -- Artists in current snapshot but not in previous
     SELECT artist_id, artist_name FROM items_current_artists WHERE artist_id IS NOT NULL
     EXCEPT DISTINCT
     SELECT artist_id, artist_name FROM items_previous_artists WHERE artist_id IS NOT NULL
),
dropped_artists AS (
     -- Artists in previous snapshot but not in current
     SELECT artist_id, artist_name FROM items_previous_artists WHERE artist_id IS NOT NULL
     EXCEPT DISTINCT
     SELECT artist_id, artist_name FROM items_current_artists WHERE artist_id IS NOT NULL
)
-- Combine all changes into one table, only if latest_snapshots has data
SELECT
    ls.snapshot_date, -- Date the change is observed (latest date)
    'Track' as item_type,
    nt.track_id as item_id,
    nt.track_name as item_name,
    'New Entry' as change_status
FROM new_tracks nt
CROSS JOIN latest_snapshots ls -- Use CROSS JOIN as latest_snapshots has only one row (or none)

UNION ALL

SELECT
    ls.snapshot_date,
    'Track' as item_type,
    dt.track_id as item_id,
    dt.track_name as item_name,
    'Dropped Off' as change_status
FROM dropped_tracks dt
CROSS JOIN latest_snapshots ls

UNION ALL

 SELECT
    ls.snapshot_date,
    'Artist' as item_type,
    na.artist_id as item_id,
    na.artist_name as item_name,
    'New Entry' as change_status
FROM new_artists na
CROSS JOIN latest_snapshots ls

UNION ALL

 SELECT
    ls.snapshot_date,
    'Artist' as item_type,
    da.artist_id as item_id,
    da.artist_name as item_name,
    'Dropped Off' as change_status
FROM dropped_artists da
CROSS JOIN latest_snapshots ls