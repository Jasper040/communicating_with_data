import streamlit as st

st.set_page_config(page_title="Executive Summary | Buying", layout="wide")

from app_shell import render_app_shell, render_scope_summary
from charts import render_bleed_chart
from data_loader import build_executive_narrative

st.header("The Bleed (Executive)")

scoped, cfg = render_app_shell(require_non_empty_scope=True)
if scoped is None:
    st.stop()
if scoped.empty:
    st.warning("No rows match current filters/horizon.")
    st.stop()

render_scope_summary(cfg, len(scoped))

horizon_days = int(cfg.get("horizon_days", 28))
narrative, kpis = build_executive_narrative(scoped, horizon_days)

with st.container(border=True):
    st.markdown("##### What leadership needs to know")
    for block in narrative:
        st.markdown(block)

with st.container(border=True):
    st.markdown("##### Key metrics")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Lost Revenue (proxy)", f"€ {kpis['lost_revenue']:,.0f}")
    c2.metric("Total Margin Eroded (proxy)", f"€ {kpis['margin_eroded']:,.0f}")
    c3.metric("Working Capital at Risk", f"€ {kpis['working_capital']:,.0f}")
    excl = float(kpis.get("missed_revenue_excluded_writeoff", 0.0))
    if excl > 0:
        st.caption(
            f"Missed-revenue signal reduced by **€ {excl:,.0f}** for SKUs treated as **written off** "
            f"(gross margin below the configured floor after discounts — see Home → Methodology)."
        )
    st.caption(
        "How these figures are computed (including proxies, the 20% markdown assumption, and how "
        "“demand” relates to sales) is documented on the **Home** page under **Methodology — how every metric is calculated**."
    )

with st.container(border=True):
    st.markdown("##### Evidence — historical buy vs true demand")
    size_groups = sorted(scoped["size_group"].dropna().unique().tolist())
    if len(size_groups) <= 1:
        render_bleed_chart(scoped, horizon_days)
    else:
        tabs = st.tabs([f"Size Group: {sg}" for sg in size_groups])
        for tab, size_group in zip(tabs, size_groups):
            with tab:
                group_df = scoped[scoped["size_group"] == size_group]
                if group_df.empty:
                    st.info(f"No rows for size group `{size_group}` in current scope.")
                else:
                    render_bleed_chart(group_df, horizon_days)
