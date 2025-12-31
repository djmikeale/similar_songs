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
            preview_url
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


if(track):
    mix_mode = st.segmented_control(
        "Filter by mix mode",
        ["perfect", "-1", "+1", "energy_boost", "scale_swap"]
    )

# ------------------------
# Query construction
# ------------------------

query = f"""
WITH selected_track AS (
    SELECT
        bpm AS selected_track_bpm
    FROM '{source}'
    {f"WHERE track_artist = ?" if track else ""}
    LIMIT 1
),
candidates AS (
    SELECT
        artists,
        track_name as track,
        bpm,
        'https://open.spotify.com/track/' || track_id as spotify_url,
        preview_url[:-38] as preview_url, -- remove query param
        genres,
        ABS(bpm - (SELECT selected_track_bpm FROM selected_track)) AS tempo_diff
    FROM '{source}'
)
SELECT *
FROM candidates
ORDER BY
    tempo_diff ASC
LIMIT 20
"""


# ------------------------
# Results
# ------------------------
# ------------------------
# Results (auto-run)
# ------------------------

if track:
    res = get_conn().execute(query, [track]).df()

    if res.empty:
        st.warning("No matches found.")
    else:
        event = st.dataframe(
            res[["artists", "track", "bpm", "spotify_url", "preview_url", "genres"]],
            column_config={
                "spotify_url": st.column_config.LinkColumn(
                    label="Spotify",
                    help="Link to the track on Spotify",
                    display_text="🔗"
                )
            },
            width="stretch",
            selection_mode="single-cell",
            on_select="rerun",
        )

if(track):
    if event.selection.cells:
        row_idx = event.selection.cells[0][0]
        preview_url = res.iloc[row_idx]["preview_url"]

        if preview_url:
            st.audio(preview_url, autoplay=True)

with st.expander("Debug info"):
    if(track):
        st.write("debug: query:", query)
        st.write("debug: params:", str([track]))
        st.write(event.selection)
