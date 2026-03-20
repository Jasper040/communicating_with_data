"""
Home entry: global page config, shared sidebar (filters + DB check), and orientation.
Analytics live under `pages/` — use the sidebar to navigate.
"""
import streamlit as st

st.set_page_config(
    page_title="Buying Control Tower",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Imports after set_page_config — data_loader uses @st.cache_data at import time.
from app_shell import render_app_shell

st.title("Buying Control Tower")
st.markdown(
    """
Welcome. Use the **sidebar** for global filters (brands, size groups, horizon) and database checks.

Open a page below for executive views, optimization, or **Ask the Data** (LLM + SQL agent).
"""
)

scoped, _cfg = render_app_shell(require_non_empty_scope=False)

if scoped is None:
    st.warning("Connect to PostgreSQL via `.streamlit/secrets.toml` to load merged buying facts.")
elif scoped.empty:
    st.info("Current filters returned no rows — widen brand/size scope or change horizon.")
else:
    st.success(f"**{len(scoped):,}** fact rows in scope — pick a page from the sidebar.")
