import streamlit as st

st.set_page_config(page_title="Optimization Engine | Buying", layout="wide")

from app_shell import render_app_shell, render_scope_summary
from charts import render_optimization_gap_and_margin, render_optimization_po_curve_comparison
from data_loader import INCREMENTAL_CONTRIBUTION_RATE, MARKDOWN_RATE, build_optimization_detail

st.header("Optimization Engine")
st.caption(
    "Compare what you **used to buy** with a **demand-optimal curve** for the **same PO quantity** — "
    "then see where rebalancing moves money."
)

scoped, cfg = render_app_shell(require_non_empty_scope=True)
if scoped is None:
    st.stop()
if scoped.empty:
    st.warning("No rows match current filters.")
    st.stop()

render_scope_summary(cfg, len(scoped))

brands = sorted(scoped["brand"].dropna().unique().tolist())
if not brands:
    st.warning("No brands available for the current global filters.")
    st.stop()

with st.container(border=True):
    st.markdown("##### Profile & PO settings")
    c1, c2, c3, c4 = st.columns(4)
    brand = c1.selectbox("Sub-brand", brands)

    brand_df = scoped[scoped["brand"] == brand].copy()
    if brand_df.empty:
        st.warning("No rows found for the selected brand in current scope.")
        st.stop()

    categories = sorted(brand_df["category"].dropna().unique().tolist()) or ["Unknown"]
    category = c2.selectbox("Category", categories)

    category_df = brand_df[brand_df["category"] == category].copy()
    fits = sorted(category_df["fit"].dropna().unique().tolist()) or ["Unknown"]
    fit = c3.selectbox("Fit", fits)

    fit_df = category_df[category_df["fit"] == fit].copy()
    size_groups = sorted(fit_df["size_group"].dropna().unique().tolist()) or ["Unknown"]
    target_qty = st.number_input("Target total buy quantity (this PO)", min_value=1, value=1000, step=10)
    size_group_options = ["All"] + size_groups if len(size_groups) > 1 else size_groups
    size_group = c4.selectbox("Size Group", size_group_options)
    style = "All"
    if size_group != "All":
        style_scope = fit_df[fit_df["size_group"] == size_group].copy()
        style_opts = ["All"] + sorted(style_scope["style"].dropna().unique().tolist())
        style = st.selectbox("Style drill-down (optional)", style_opts)
    else:
        st.caption("Showing all size groups as separate visuals (style drill-down disabled in this mode).")


def render_profile(selected_size_group: str, selected_style: str) -> None:
    detail, meta = build_optimization_detail(
        scoped,
        brand,
        category,
        fit,
        selected_size_group,
        int(target_qty),
        selected_style,
    )
    if detail.empty:
        st.warning("No rows found for this profile.")
        return

    st.markdown(f"### **{meta['profile_label']}**")
    if meta["fallback_used"]:
        st.warning("Low sample: using broader baseline — interpret the optimal curve cautiously.")

    with st.container(border=True):
        st.markdown("##### At a glance")
        hero1, hero2, hero3, hero4 = st.columns(4)
        hero1.metric("Target PO quantity", f"{meta['target_qty']:,}")
        hero2.metric(
            "Projected € impact (rebalancing heuristic)",
            f"€ {meta['total_projected_margin_eur']:,.0f}",
            help=(
                f"Sum of per-size estimates: extra units × list × {INCREMENTAL_CONTRIBUTION_RATE:.0%} contribution; "
                f"fewer units × list × {MARKDOWN_RATE:.0%} avoided markdown."
            ),
        )
        hero3.metric("Confidence", meta["confidence"])
        hero4.metric("Sold units (sample)", f"{meta['sold_units']:,.0f}")

    with st.expander("How we estimate € impact (transparent assumptions)", expanded=False):
        st.markdown(
            f"""
- **Historical curve**: your observed **purchase mix** by size, scaled to the target quantity with **largest remainder** rounding (same as the optimal curve).
- **Optimal curve**: **demand mix** (`sales_qty`) by size, same target quantity and rounding — this is the *reallocation* the engine recommends.
- **Per size**: if the optimal curve **adds** units vs the historical curve at this PO size, we value that lift at **{INCREMENTAL_CONTRIBUTION_RATE:.0%}** of list price (incremental contribution).
  If it **trims** units, we value **markdown risk avoided** at **{MARKDOWN_RATE:.0%}** of list price.
- This is a **planning heuristic**, not a forecast — it’s designed to be defensible in a buying meeting.
"""
        )

    with st.container(border=True):
        st.markdown("##### 1. Curve comparison (same PO quantity)")
        render_optimization_po_curve_comparison(detail)

    with st.container(border=True):
        st.markdown("##### 2. Gap and projected margin by size")
        render_optimization_gap_and_margin(detail)

    with st.container(border=True):
        st.markdown("##### 3. Action table")
        display = detail.assign(
            hist_po_pct=lambda d: (d["po_share_historical_curve"] * 100).round(1),
            optimal_po_pct=lambda d: (d["po_share_optimal_curve"] * 100).round(1),
            hist_buy_mix_pct=lambda d: (d["hist_buy_share"] * 100).round(1),
            demand_mix_pct=lambda d: (d["optimal_demand_share"] * 100).round(1),
        )[
            [
                "Size",
                "hist_buy_mix_pct",
                "demand_mix_pct",
                "hist_po_pct",
                "optimal_po_pct",
                "variance_pp",
                "qty_historical_at_target",
                "qty_optimal_at_target",
                "delta_qty",
                "avg_list_price",
                "projected_margin_eur",
            ]
        ].rename(
            columns={
                "hist_buy_mix_pct": "Hist buy mix %",
                "demand_mix_pct": "Demand mix %",
                "hist_po_pct": "Hist PO % @ target",
                "optimal_po_pct": "Optimal PO % @ target",
                "variance_pp": "Δ PO share (pp)",
                "qty_historical_at_target": "Units (hist curve)",
                "qty_optimal_at_target": "Units (optimal)",
                "delta_qty": "Δ units",
                "avg_list_price": "Avg list €",
                "projected_margin_eur": "Proj. € impact",
            }
        )

        st.dataframe(
            display,
            width="stretch",
            hide_index=True,
            column_config={
                "Hist buy mix %": st.column_config.NumberColumn(format="%.1f"),
                "Demand mix %": st.column_config.NumberColumn(format="%.1f"),
                "Hist PO % @ target": st.column_config.NumberColumn(format="%.1f"),
                "Optimal PO % @ target": st.column_config.NumberColumn(format="%.1f"),
                "Δ PO share (pp)": st.column_config.NumberColumn(format="%.1f"),
                "Units (hist curve)": st.column_config.NumberColumn(format="%d"),
                "Units (optimal)": st.column_config.NumberColumn(format="%d"),
                "Δ units": st.column_config.NumberColumn(format="%d"),
                "Avg list €": st.column_config.NumberColumn(format="%.2f"),
                "Proj. € impact": st.column_config.NumberColumn(format="%.0f"),
            },
        )

        csv = detail.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download full optimization detail (CSV)",
            csv,
            file_name=f"optimization_detail_{brand}_{category}_{fit}_{selected_size_group}.csv".replace(" ", "_"),
            mime="text/csv",
            key=f"download_opt_{selected_size_group}",
        )


if size_group == "All":
    tabs = st.tabs([f"Size Group: {sg}" for sg in size_groups])
    for tab, sg in zip(tabs, size_groups):
        with tab:
            render_profile(sg, "All")
else:
    render_profile(size_group, style)
