import streamlit as st

st.set_page_config(page_title="Demand Analysis | Buying", layout="wide")

from app_shell import render_app_shell, render_scope_summary
from charts import render_mismatch_chart

st.header("The Mismatch")

scoped, cfg = render_app_shell(require_non_empty_scope=True)
if scoped is None:
    st.stop()
if scoped.empty:
    st.warning("No rows available for current global filters.")
    st.stop()

render_scope_summary(cfg, len(scoped))

with st.container(border=True):
    st.markdown("##### Slice the mismatch")
    fc1, fc2 = st.columns(2)
    categories = sorted(scoped["category"].dropna().unique().tolist())
    category = fc1.selectbox("Category", ["All"] + categories)

    df = scoped.copy()
    if category != "All":
        df = df[df["category"] == category]

    fit_options = sorted(df["fit"].dropna().unique().tolist())
    fit = fc2.selectbox("Fit", ["All"] + fit_options)
    if fit != "All":
        df = df[df["fit"] == fit]

size_groups = sorted(df["size_group"].dropna().unique().tolist())
if len(size_groups) <= 1:
    mismatch = df.groupby("size", dropna=False)[["buy_qty", "sales_qty"]].sum().reset_index()
    mismatch["buy_share"] = mismatch["buy_qty"] / max(mismatch["buy_qty"].sum(), 1)
    mismatch["demand_share"] = mismatch["sales_qty"] / max(mismatch["sales_qty"].sum(), 1)
    mismatch["gap_pp"] = (mismatch["demand_share"] - mismatch["buy_share"]) * 100

    with st.container(border=True):
        render_mismatch_chart(mismatch)
    with st.container(border=True):
        st.markdown("##### Detail by size")
        st.dataframe(
            mismatch.rename(
                columns={
                    "buy_qty": "buy_units",
                    "sales_qty": "demand_units",
                    "gap_pp": "gap_percentage_points",
                }
            ),
            use_container_width=True,
        )
else:
    tabs = st.tabs([f"Size Group: {sg}" for sg in size_groups])
    for tab, size_group in zip(tabs, size_groups):
        with tab:
            group_df = df[df["size_group"] == size_group]
            if group_df.empty:
                st.info(f"No rows for size group `{size_group}` in current scope.")
                continue

            mismatch = group_df.groupby("size", dropna=False)[["buy_qty", "sales_qty"]].sum().reset_index()
            mismatch["buy_share"] = mismatch["buy_qty"] / max(mismatch["buy_qty"].sum(), 1)
            mismatch["demand_share"] = mismatch["sales_qty"] / max(mismatch["sales_qty"].sum(), 1)
            mismatch["gap_pp"] = (mismatch["demand_share"] - mismatch["buy_share"]) * 100

            with st.container(border=True):
                render_mismatch_chart(mismatch)
            with st.container(border=True):
                st.markdown("##### Detail by size")
                st.dataframe(
                    mismatch.rename(
                        columns={
                            "buy_qty": "buy_units",
                            "sales_qty": "demand_units",
                            "gap_pp": "gap_percentage_points",
                        }
                    ),
                    use_container_width=True,
                )
