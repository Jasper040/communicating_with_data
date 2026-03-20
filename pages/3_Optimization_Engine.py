import streamlit as st

st.set_page_config(page_title="Optimization Engine | Buying", layout="wide")

from app_shell import render_app_shell
from data_loader import profile_recommendation

st.header("Optimization Engine")

scoped, _cfg = render_app_shell(require_non_empty_scope=True)
if scoped is None:
    st.stop()
if scoped.empty:
    st.warning("No rows match current filters.")
    st.stop()

brands = sorted(scoped["brand"].dropna().unique().tolist())
if not brands:
    st.warning("No brands available for the current global filters.")
    st.stop()

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
size_group = c4.selectbox("Size Group", size_groups)

style_scope = fit_df[fit_df["size_group"] == size_group].copy()
style_opts = ["All"] + sorted(style_scope["style"].dropna().unique().tolist())
style = st.selectbox("Style drill-down", style_opts)
target_qty = st.number_input("Target total buy quantity", min_value=1, value=1000, step=10)

rec, meta = profile_recommendation(scoped, brand, category, fit, size_group, int(target_qty), style)
if rec.empty:
    st.warning("No rows found for this profile.")
    st.stop()
if meta["fallback_used"]:
    st.warning("Low sample: using broader baseline.")
st.caption(
    f"Confidence: {meta['confidence']} | Sold units: {meta['sold_units']:.0f} | "
    f"Coverage: {meta.get('coverage', 0):.1%} | Freshness: {meta.get('freshness_days', 999)} days"
)
out = rec.rename(columns={"size": "Size", "recommended_share": "Share", "recommended_qty": "Buy Qty"})[
    ["Size", "Share", "Buy Qty"]
]
st.dataframe(out, use_container_width=True)
st.download_button(
    "Download recommendation (CSV)",
    out.to_csv(index=False).encode("utf-8"),
    file_name=f"recommended_size_curve_{brand}_{category}_{fit}.csv".replace(" ", "_"),
    mime="text/csv",
)
