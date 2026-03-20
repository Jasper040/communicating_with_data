import streamlit as st

st.set_page_config(page_title="Action Queue | Buying", layout="wide")

from app_shell import render_app_shell
from data_loader import normalize_0_100

st.header("Action Queue")

scoped, cfg = render_app_shell(require_non_empty_scope=True)
if scoped is None:
    st.stop()
if scoped.empty:
    st.warning("No rows match current filters.")
    st.stop()

rank_mode = cfg["rank_mode"]
confidence_floor = cfg["min_confidence"]

profile = (
    scoped.groupby(["brand", "category", "fit", "size_group"], dropna=False)
    .agg(
        sold_units=("sales_qty", "sum"),
        buy_units=("buy_qty", "sum"),
        stock_units=("stock_qty", "sum"),
        avg_price=("list_price", "mean"),
    )
    .reset_index()
)
profile["missed_revenue_raw"] = (profile["sold_units"] - profile["stock_units"]).clip(lower=0) * profile["avg_price"]
profile["markdown_risk_raw"] = (profile["stock_units"] - profile["sold_units"]).clip(lower=0) * profile["avg_price"] * 0.2
profile["mismatch_raw"] = (profile["buy_units"] - profile["sold_units"]).abs()
profile["missed_revenue_score"] = normalize_0_100(profile["missed_revenue_raw"])
profile["markdown_risk_score"] = normalize_0_100(profile["markdown_risk_raw"])
profile["mismatch_severity_score"] = normalize_0_100(profile["mismatch_raw"])
profile["blended_score"] = (
    profile["missed_revenue_score"] * 0.40
    + profile["markdown_risk_score"] * 0.35
    + profile["mismatch_severity_score"] * 0.25
)
profile["confidence"] = profile["sold_units"].apply(lambda x: "High" if x >= 300 else ("Medium" if x >= 100 else "Low"))
floor_map = {"Low": 0, "Medium": 1, "High": 2}
profile = profile[profile["confidence"].map(floor_map) >= floor_map[confidence_floor]]
score_col = {
    "Blended": "blended_score",
    "Missed Revenue": "missed_revenue_score",
    "Markdown Risk": "markdown_risk_score",
    "Mismatch Severity": "mismatch_severity_score",
}[rank_mode]
profile = profile.sort_values(score_col, ascending=False)
profile["suggested_action"] = "Rebalance buy curve for next PO"
output = profile[
    ["brand", "category", "fit", "size_group", score_col, "confidence", "suggested_action"]
].rename(columns={score_col: "priority_score"})
st.dataframe(output, use_container_width=True)
st.download_button(
    "Export action shortlist",
    output.to_csv(index=False).encode("utf-8"),
    file_name="action_queue.csv",
    mime="text/csv",
)
