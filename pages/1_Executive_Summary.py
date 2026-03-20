import streamlit as st

st.set_page_config(page_title="Executive Summary | Buying", layout="wide")

from app_shell import render_app_shell
from charts import render_bleed_chart
from data_loader import build_executive_narrative

st.header("The Bleed (Executive)")

scoped, cfg = render_app_shell(require_non_empty_scope=True)
if scoped is None:
    st.stop()
if scoped.empty:
    st.warning("No rows match current filters/horizon.")
    st.stop()

horizon_days = int(cfg.get("horizon_days", 28))
narrative, kpis = build_executive_narrative(scoped, horizon_days)

st.markdown("## What leadership needs to know")
for block in narrative:
    st.markdown(block)

c1, c2, c3 = st.columns(3)
c1.metric("Total Lost Revenue (proxy)", f"€ {kpis['lost_revenue']:,.0f}")
c2.metric("Total Margin Eroded (proxy)", f"€ {kpis['margin_eroded']:,.0f}")
c3.metric("Working Capital at Risk", f"€ {kpis['working_capital']:,.0f}")

st.markdown("### Evidence")
render_bleed_chart(scoped, horizon_days)
