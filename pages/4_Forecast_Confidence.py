import pandas as pd
import streamlit as st

st.set_page_config(page_title="Forecast & Confidence | Buying", layout="wide")

from app_shell import render_app_shell
from charts import render_forecast_chart
from data_loader import REQUIRED_KEYS, confidence_label

st.header("Forecast & Confidence")

scoped, cfg = render_app_shell(require_non_empty_scope=True)
if scoped is None:
    st.stop()
if scoped.empty:
    st.info("No demand rows available for this scope.")
    st.stop()

horizon_days = int(cfg.get("horizon_days", 28))
render_forecast_chart(scoped, horizon_days)

freshness = (
    (pd.Timestamp.today() - scoped["order_date"].max()).days
    if not scoped["order_date"].isna().all()
    else 999
)
coverage = len(scoped[REQUIRED_KEYS + ["sales_qty"]].dropna()) / max(len(scoped), 1)
conf = confidence_label(float(scoped["sales_qty"].sum()), coverage, int(freshness), False)
st.info(
    f"Confidence: {conf} | Sold units: {scoped['sales_qty'].sum():.0f} | "
    f"Coverage: {coverage:.1%} | Freshness: {freshness} days"
)
