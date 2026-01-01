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
    [""] + df["track_artist"].tolist(),
    index=None,
    placeholder="Pick a track to find similar ones...",
    label_visibility="hidden"
)

mix_mode = None
if(track):
    mix_mode_map = {
         "perfect":"✔️ perfect",
         "-1":"➕ -1",
         "+1":"➖ +1 ",
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
WITH selected_track AS (
    SELECT
        bpm AS selected_track_bpm,
        key AS selected_key
    FROM '{source}'
    {f"WHERE track_artist = ?" if track else ""}
    LIMIT 1
),
harmonic AS (
    WITH
    m AS (
        SELECT
            k,
            dm,
            k || dm AS key
        FROM range(1,13) t(k)
        CROSS JOIN (SELECT UNNEST(['d','m']) AS dm)
    )
    SELECT
        key AS input_key,
        key AS perfect_mix,
        CAST(k % 12 + 1 AS VARCHAR) || dm AS plus_1,
        CAST(CASE WHEN k = 1 THEN 12 ELSE k - 1 END AS VARCHAR) || dm AS minus_1,
        CAST(k % 12 + 2 AS VARCHAR) || dm AS energy_boost,
        CAST(k AS VARCHAR) || CASE WHEN dm = 'm' THEN 'd' ELSE 'm' END AS scale_swap
    FROM m
),
candidates AS (
    SELECT
        artists,
        track_name as track,
        bpm,
        key,
        'https://open.spotify.com/track/' || track_id as spotify_url,
        preview_url[:-38] as preview_url, -- remove query param
        genres,
        ABS(bpm - (SELECT selected_track_bpm FROM selected_track)) AS tempo_diff
    FROM '{source}'
)
SELECT
    c.*
FROM candidates c

{f"""
JOIN selected_track s ON TRUE
JOIN harmonic h ON h.input_key = s.selected_key
WHERE
    c.key =
    CASE '{mix_mode}'
        WHEN 'perfect' THEN h.perfect_mix
        WHEN '+1' THEN h.plus_1
        WHEN '-1' THEN h.minus_1
        WHEN 'energy_boost' THEN h.energy_boost
        WHEN 'scale_swap' THEN h.scale_swap
    END
""" if mix_mode else ""}

ORDER BY tempo_diff
LIMIT 20
"""


# ------------------------
# Results
# ------------------------

if track:
    res = get_conn().execute(query, [track]).df()

    if res.empty:
        st.warning("No matches found.")
    else:
        st.caption(
            f"🎼 Selected key: **{res.iloc[0]['key']}** · Mode: **{mix_mode}**"
        )

        event = st.dataframe(
            res[["artists", "track", "key", "bpm", "spotify_url", "preview_url", "genres"]],
            column_config={
                "spotify_url": st.column_config.LinkColumn(
                    label="Spotify",
                    display_text="🔗"
                )
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

with st.expander("Debug info"):
    if(track):
        st.write("debug: query:", query)
        st.write("debug: params:", str([track]))
        st.write(event.selection)
