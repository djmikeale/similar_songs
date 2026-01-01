# app.py
import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="Next stop Uranus", page_icon="🚀")

source = "dnb.parquet"

@st.cache_resource
def get_conn():
    return duckdb.connect()

cached_sql = f"""
        SELECT
        track_name,
        artists,
        track_artist,
        track_id,
        bpm,
        genres,
        preview_url,
        key
        FROM '{source}'
    """

@st.cache_data
def load_metadata():
    return get_conn().execute(cached_sql).df()
df = load_metadata()


st.title("🌍 → 🚀 → 🌙 → ☄️ → 🌌 → 🌠 → 🪐")

# ------------------------
# Inputs
# ------------------------
track = st.selectbox(
    "Select a track",
    df["track_artist"].tolist(),
    index=None,
    placeholder="Pick a track to find similar ones...",
    label_visibility="hidden"
)

mix_mode = None
if(track):
    mix_mode_map = {
         "perfect":"✔️ perfect",
         "minus_1":"➖ -1",
         "plus_1":"➕ +1 ",
         "energy_boost":"🚀 energy_boost",
         "scale_swap":"🔃 scale_swap"
    }
    mix_mode = st.segmented_control(
        "Select mix mode",
        options=mix_mode_map.keys(),
        format_func=lambda mix_mode: mix_mode_map[mix_mode],
        width="stretch",
    )
# ------------------------
# Query construction
# ------------------------

query = f"""
with
    source as (
        select
        *,
        --input key indicates musical key. 0 = C, 1 = C♯/D♭, 2 = D, ..., 11 = B
        --transform into Camelot notation for easier DJing
        map(
            [0,1, 2,3, 4,5,6,7,8, 9,10,11],
            [8,3,10,5,12,7,2,9,4,11, 6, 1]
        )[key[:-2]::int] || case when key[-1:] = 'm' then 'A' else 'B' end AS camelot_key
        from '{source}'),

    selected_track as (
        select
            bpm as selected_track_bpm,
            camelot_key as selected_key,
            track_artist as selected_track_artist
        from source
        {f"WHERE track_artist = ?" if track else ""}
        limit 1
    ),
    harmonic as (
        select
            k || dm as input_key,
            k || dm as perfect,
            k % 12 + 1 || dm as plus_1,
            case when k = 1 then 12 else k - 1 end || dm as minus_1,
            k % 12 + 2 || dm as energy_boost,
            k || case when dm = 'A' then 'B' else 'A' end as scale_swap
        from range(1, 13) t(k)
        cross join (select unnest(['A', 'B']) as dm)
    ),
    final as (
        select
            artists,
            track_name as track,
            bpm,
            camelot_key,
            'https://open.spotify.com/track/' || track_id as spotify_url,
            preview_url[:-38] as preview_url,  -- remove query param
            genres
        from source
        join selected_track s on true

        {f"""
        join harmonic h on h.input_key = s.selected_key
        where
            camelot_key = case
                '{mix_mode}'
                when 'perfect'
                then h.perfect
                when 'plus_1'
                then h.plus_1
                when 'minus_1'
                then h.minus_1
                when 'energy_boost'
                then h.energy_boost
                when 'scale_swap'
                then h.scale_swap
            end
            --we always wanna keep the selected track in results
            or selected_track_artist = track_artist
        """ if mix_mode else ""}
        order by
            --ensure selected track is on top, followed by best bpm matches
            selected_track_artist = track_artist desc, abs(bpm - selected_track_bpm)
        limit 20
    )
select *
from final
"""


with st.expander("Debug info"):
    if(track):
        st.write("debug: query:", query)
        st.write("debug: params:", str([track]))
        #st.write(event.selection)

# ------------------------
# Results
# ------------------------

if track:
    res = get_conn().execute(query, [track]).df()

    if res.empty:
        st.warning("No matches found.")
    else:
        event = st.dataframe(
            res[["artists", "track", "camelot_key", "bpm", "spotify_url", "preview_url", "genres"]],
            column_config={
                "camelot_key":"Key",
                "track":"Track",
                "artists":"Artists",
                "bpm":"BPM",
                "genres":"Genres",
                "spotify_url": st.column_config.LinkColumn(
                    label="Spotify",
                    display_text="🔗"
                ),
            },
            width="stretch",
            selection_mode="single-cell",
            on_select="rerun",
        )

        if event.selection.cells:
            row_idx = event.selection.cells[0][0]
            preview = res.iloc[row_idx]["preview_url"]
            if preview:
                st.audio(preview, autoplay=True)
