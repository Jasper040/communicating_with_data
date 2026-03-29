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
narrative, kpis = build_executive_narrative(scoped, horizon_days, cfg.get("as_of"))

with st.container(border=True):
    st.markdown("##### What leadership needs to know")
    for block in narrative:
        st.markdown(block)

with st.container(border=True):
    st.markdown("##### Missed revenue from stock-outs")
    st.caption(
        "**Component A** of *Total Lost Revenue (proxy)* — see **Key metrics**. Horizon / run-rate gap only when "
        "**realized gross margin is below the write-off floor** (paid-through lines, not giveaways), valued at list, "
        "then **aged out** after **20 weeks** from first sale vs **as-of**."
    )
    s_eur = float(kpis.get("stockout_missed_revenue_eur", 0.0))
    s_skus = int(kpis.get("stockout_skus_with_gap", 0))
    s_dem = float(kpis.get("stockout_expected_demand_units", 0.0))
    z_eur = float(kpis.get("stockout_zero_inventory_missed_eur", 0.0))
    m1, m2 = st.columns(2)
    m1.metric("Stock-out missed revenue (proxy)", f"€ {s_eur:,.0f}")
    m2.metric("SKU-size rows with demand > stock", f"{s_skus:,}")
    if s_dem > 0:
        st.caption(f"Scope-level implied demand over the horizon: **{s_dem:,.0f}** units (vs. sum of on-hand rows).")
    if z_eur > 0:
        st.caption(
            f"**€ {z_eur:,.0f}** of that sits on rows with **no reported stock** but **positive sales** "
            "(stronger out-of-stock signal)."
        )
    age_n = int(kpis.get("stockout_skus_age_written_off", 0))
    if age_n > 0:
        st.caption(f"**{age_n:,}** SKU-size rows are excluded here only (first sale + 20 weeks before as-of).")
    elif s_eur <= 0:
        st.info(
            "No stock-out € in this scope: either no demand-vs-stock gap, no SKUs below the margin floor with a "
            "paid-through price signal, or rows are aged out — see Methodology."
        )

with st.container(border=True):
    st.markdown("##### Key metrics")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Lost Revenue (proxy)", f"€ {kpis['lost_revenue']:,.0f}")
    c2.metric("Total Margin Eroded (proxy)", f"€ {kpis['margin_eroded']:,.0f}")
    c3.metric("Working Capital at Risk", f"€ {kpis['working_capital']:,.0f}")
    so = float(kpis.get("stockout_missed_revenue_eur", 0.0))
    me = float(kpis.get("margin_eroded", 0.0))
    st.caption(
        f"**Total lost revenue** = stock-out missed revenue (**€ {so:,.0f}**) + margin eroded (**€ {me:,.0f}**). "
        "See the stock-outs section and Methodology."
    )
    st.caption(
        "How these figures are computed (including proxies, the 30% markdown assumption, and how "
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
