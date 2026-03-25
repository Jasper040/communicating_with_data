import pandas as pd
import streamlit as st

st.set_page_config(page_title="Forecast & Confidence | Buying", layout="wide")

from app_shell import render_app_shell, render_scope_summary
from charts import render_forecast_chart
from data_loader import REQUIRED_KEYS, confidence_label

st.header("Forecast & Confidence")

scoped, cfg = render_app_shell(require_non_empty_scope=True)
if scoped is None:
    st.stop()
if scoped.empty:
    st.info("No demand rows available for this scope.")
    st.stop()

render_scope_summary(cfg, len(scoped))

horizon_days = int(cfg.get("horizon_days", 28))
size_groups = sorted(scoped["size_group"].dropna().unique().tolist())
if len(size_groups) <= 1:
    with st.container(border=True):
        render_forecast_chart(scoped, horizon_days)

    freshness = (
        (pd.Timestamp.today() - scoped["order_date"].max()).days
        if not scoped["order_date"].isna().all()
        else 999
    )
    coverage = len(scoped[REQUIRED_KEYS + ["sales_qty"]].dropna()) / max(len(scoped), 1)
    conf = confidence_label(float(scoped["sales_qty"].sum()), coverage, int(freshness), False)
    with st.container(border=True):
        st.markdown("##### Signal quality")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Confidence", conf)
        k2.metric("Sold units (sample)", f"{scoped['sales_qty'].sum():,.0f}")
        k3.metric("Key coverage", f"{coverage:.1%}")
        k4.metric("Data freshness", f"{freshness} days")
else:
    tabs = st.tabs([f"Size Group: {sg}" for sg in size_groups])
    for tab, size_group in zip(tabs, size_groups):
        with tab:
            group_df = scoped[scoped["size_group"] == size_group]
            if group_df.empty:
                st.info(f"No rows for size group `{size_group}` in current scope.")
                continue

            with st.container(border=True):
                render_forecast_chart(group_df, horizon_days)
            freshness = (
                (pd.Timestamp.today() - group_df["order_date"].max()).days
                if not group_df["order_date"].isna().all()
                else 999
            )
            coverage = len(group_df[REQUIRED_KEYS + ["sales_qty"]].dropna()) / max(len(group_df), 1)
            conf = confidence_label(float(group_df["sales_qty"].sum()), coverage, int(freshness), False)
            with st.container(border=True):
                st.markdown("##### Signal quality")
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Confidence", conf)
                k2.metric("Sold units (sample)", f"{group_df['sales_qty'].sum():,.0f}")
                k3.metric("Key coverage", f"{coverage:.1%}")
                k4.metric("Data freshness", f"{freshness} days")
