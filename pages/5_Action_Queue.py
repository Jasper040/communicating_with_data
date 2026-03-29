import streamlit as st

st.set_page_config(page_title="Action Queue | Buying", layout="wide")

from app_shell import render_app_shell, render_scope_summary
from charts import render_action_queue_priority_chart
from data_loader import MARKDOWN_RATE, missed_revenue_weight, normalize_0_100

st.header("Action Queue")

scoped, cfg = render_app_shell(require_non_empty_scope=True)
if scoped is None:
    st.stop()
if scoped.empty:
    st.warning("No rows match current filters.")
    st.stop()

render_scope_summary(cfg, len(scoped))

rank_mode = cfg["rank_mode"]
confidence_floor = cfg["min_confidence"]

scoped = scoped.copy()
scoped["_missed_rev_row"] = (
    (scoped["sales_qty"] - scoped["stock_qty"]).clip(lower=0)
    * scoped["list_price"]
    * missed_revenue_weight(scoped)
)
profile = (
    scoped.groupby(["brand", "category", "fit", "size_group"], dropna=False)
    .agg(
        sold_units=("sales_qty", "sum"),
        buy_units=("buy_qty", "sum"),
        stock_units=("stock_qty", "sum"),
        avg_price=("list_price", "mean"),
        missed_revenue_raw=("_missed_rev_row", "sum"),
    )
    .reset_index()
)
profile["markdown_risk_raw"] = (
    (profile["stock_units"] - profile["sold_units"]).clip(lower=0) * profile["avg_price"] * MARKDOWN_RATE
)
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
with st.container(border=True):
    st.markdown("##### Priority ranking (top profiles)")
    render_action_queue_priority_chart(output, top_n=15)
with st.container(border=True):
    st.markdown("##### Full shortlist")
    st.dataframe(output, use_container_width=True)
st.download_button(
    "Export action shortlist",
    output.to_csv(index=False).encode("utf-8"),
    file_name="action_queue.csv",
    mime="text/csv",
)
