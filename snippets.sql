-- drop table artist_images
-- drop table artist_albums
-- drop table available_markets
-- drop table album_images

--INSTALL sqlite;
--LOAD sqlite;
--attach 'spotify_clean.sqlite3' (TYPE sqlite);

duckdb 'spotify_clean.sqlite3'

copy (
    select rowid as artist_rowid, name as artist_name, popularity as artist_popularity
    from spotify_clean.artists
    where followers_total > 100
) to 'artists.parquet'
  (FORMAT parquet, COMPRESSION zstd);

copy (
    select * from spotify_clean.track_artists
) to 'track_artists.parquet'
  (FORMAT parquet, COMPRESSION zstd);

copy(
    select
        array_agg(a.artist_name order by a.artist_popularity desc) as artist_names,
        sum(a.artist_popularity) as artists_popularity,
        ta.track_rowid,
    from 'track_artists.parquet' ta
    inner join
        'artists.parquet' a on a.artist_rowid = ta.artist_rowid and a.artist_popularity > 100
    group by all
) to 'artists_agg.parquet' (format parquet, compression zstd);





with
    artist_filter as (
        select
            rowid as artist_rowid, name as artist_name, popularity as artist_popularity
        from spotify_clean.artists
        where followers_total > 100 --and rowid in (378468, 4252114, 4727686, 839619)
    ),

    tracks_filter as (
        select
            rowid as track_rowid,
            id,
            name as track_name,
            external_id_isrc,
            popularity,
            duration_ms
        from spotify_clean.tracks
        where duration_ms > 120000 and duration_ms < 720000 --and rowid = 723756
    ),

    tracks_artists as (
        select ta.track_rowid, ta.artist_rowid from spotify_clean.track_artists ta
    ),

    join_tracks_artists as (
        select
            tf.track_rowid,
            tf.id as track_id,
            listagg(a.artist_name) as artist_names,
            avg(a.artist_popularity) as avg_artist_popularity,
            tf.track_name,
            tf.external_id_isrc,
            tf.popularity as track_popularity,
            tf.duration_ms
        from tracks_filter tf
        inner join tracks_artists ta on ta.track_rowid = tf.track_rowid
        inner join artist_filter a on a.artist_rowid = ta.artist_rowid
        group by all
    )

select *
from join_tracks_artists
;


select * exclude (track_rowid_1)
from 'all.parquet'
qualify row_number() over (partition by external_id_isrc order by popularity desc) = 1


select * exclude (time_signature)
from 'song_details.parquet'
where time_signature = 4
and danceability > 0.4
and energy > 0.4
and speechiness < 0.8
and liveness < 0.8



select * exclude dt.track_id
from all_filtered.parquet a
inner join track_details.parquet dt on dt.track_id = a.id limit 10

copy (select track_rowid,genre from artist_genres
 inner join track_artists on artist_genres.artist_rowid = track_artists.artist_rowid
where genre in ('psytrance','dancehall','riddim','progressive house','afro house','drum and bass','breakbeat','tech house','hard techno','trance','minimal techno','hyperpop','underground hip hop','jungle','progressive trance','melodic techno','synthwave','dubstep','melodic house','acid techno','nu disco','tribal house','hard house','latin house','speedcore','latin hip hop','future bass','tekno','hardstyle','deep house','brazilian hip hop','funky house','bass house','brazilian trap','bass music','frenchcore','mexican hip hop','chinese hip hop','dark trap','future house','lo-fi house','witch house','miami bass','electro house','moombahton','drumstep','slap house','hypertechno','tropical house','electro swing')
) to 'genre.parquet' (format parquet, compression zstd)



select id, artist_names, track_name from a_g.parquet where array_contains(genres,'drum and bass') and key = 7 and mode = 1 and tempo between 173 and 175 limit 100;


copy (select distinct
  ar.track_rowid,
  id as track_id,
  ar.artists,
  track_name || ' | ' || array_to_string(artists, ', ') as track_artist,
  avg_artist_popularity::tinyint as avg_artist_popularity,
  genres,
  track_name,
    preview_url,
  key || case
    when mode then 'd'
    else 'm'
  end as "key",
  tempo::smallint as bpm,
  (danceability * 100)::tinyint as danceability,
  (energy * 100)::tinyint as energy,
  (liveness * 100)::tinyint as liveness,
  (valence * 100)::tinyint as valence,

from
  'all.parquet' al
  left join 'artists.parquet' ar on al.track_rowid = ar.track_rowid
  left join 'tracks.parquet' t on al.track_rowid = t.rowid)
  to all_transformed.parquet (format parquet, compression zstd);
