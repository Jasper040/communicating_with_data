"""
Home entry: global page config, shared sidebar (filters + DB check), and orientation.
Analytics live under `pages/` — use the sidebar to navigate.
"""
import streamlit as st

st.set_page_config(
    page_title="Buying Control Tower",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Imports after set_page_config — data_loader uses @st.cache_data at import time.
from app_shell import render_app_shell

st.title("Buying Control Tower")
st.markdown(
    """
Welcome. Use the **sidebar** for global filters (brands, size groups, horizon) and database checks.

Open a page below for executive views, optimization, or **Ask the Data** (LLM + SQL agent).
"""
)

with st.expander("Methodology — how every metric is calculated", expanded=False):
    st.markdown(
        """
### Data grain and sources

- **Grain:** one row per `(item_no, colour_no, size, barcode)` after merging PostgreSQL tables.
- **Merge:** keys are the union of rows from `total_sales_b2c`, `inventory`, `purchased`, and `products`. Sales, inventory, and purchase quantities are **summed** per key; product attributes use one row per key (highest list price when duplicates exist).
- **Fields used:** B2C sales quantity and latest order date; on-hand stock; purchased quantity and purchase price; product list price, brand, season, category (`item_group`), fit, and `size_group`.

### Common column definitions (`with_common_metrics`)

| Column | Meaning |
|--------|--------|
| **Demand (sales_qty)** | Sum of B2C sales units in the merged fact for that SKU. |
| **Buy (buy_qty)** | Sum of purchased units for that SKU. |
| **Stock (stock_qty)** | On-hand inventory units for that SKU. |
| **List price** | Product **sales listprice** (incl. **21% NL VAT**). |
| **List price ex VAT** | `list_price ÷ 1.21` (override divisor with **`NL_VAT_DIVISOR`**). Used for ticket value vs ex-VAT revenue. |
| **Unit cost** | Purchase price when present and > 0; otherwise **50% of list price** as a fallback. |
| **Sales revenue (ex VAT)** | `SUM` of the ex-VAT amount column from `total_sales_b2c` per SKU (default **`PG_SALES_EX_VAT_AMOUNT_COL` = `amount_lcy`**). Set **`PG_SALES_REVENUE_COL`** to sum one precomputed column instead. |
| **Implied discount** | `max(0, sales_qty × list_price_ex_vat − sales_revenue)` — no discount field; ticket from products vs actual ex-VAT revenue. |
| **Season / brand** | Taken from product first; if missing, from sales or purchase metadata. |

Global filters (brands, size groups, horizon window) **restrict which rows** enter every chart and KPI.

---

### Executive Summary — three headline KPIs (`compute_kpis`)

All three use the **same filtered scope** and **horizon length** (days) from the sidebar.

1. **Total Lost Revenue (proxy)** — **sum of two components** (same scope and horizon):  
   - **Margin eroded:** per row `max(stock_qty − sales_qty, 0) × list_price × MARKDOWN_RATE` (default **30%** markdown on excess units).  
   - **Working capital at risk:** for rows with sell-through **< 30%**, `stock_qty × unit_cost` (sell-through = `sales_qty / stock_qty`, with zero stock handled safely).  
   - **Total lost revenue = margin eroded + working capital at risk** (`compute_kpis`). *Interpretation:* combined stylized proxies for markdown risk on overstock and capital tied up in slow movers; not a literal P&L reconciliation.

2. **Total Margin Eroded (proxy)** — same as the margin-eroded component above (also shown separately in the UI).

3. **Working Capital at Risk** — same as the WC component above (also shown separately in the UI).

**Missed revenue from stock-outs** (dedicated panel) is a **separate** proxy, **not** included in *Total Lost Revenue*: same **horizon** and `expected_units` from `stockout_missed_revenue_stats`, list-price gap with `margin_below_writeoff_floor_for_stockout` and **age** — rows get **zero** stock-out weight when **as-of** is after `sales_first_order_date` + **`STOCKOUT_WRITE_OFF_WEEKS_AFTER_FIRST_SALE`** weeks (default **8**, env override). **Rows with no on-hand stock but positive sales are excluded** from the stock-out €.

---

### Executive narrative bullets (`build_executive_narrative`)

These **do not** necessarily reconcile to the three headline totals; they highlight **where** signals concentrate (by size / segment).

- **Demand not covered by stock:** Row proxy `(sales_qty − stock_qty).clip(lower=0) × list_price × write-off weight` (same margin rule as headline), aggregated by **size** — largest bar drives the bullet.  
- **Markdown pressure:** Row proxy `max(stock − sales, 0) × list × 30%`, aggregated by **size**.  
- **Buy curve vs demand:** Within the scope, `buy_share` = share of total `buy_qty` by size; `demand_share` = share of total `sales_qty` by size. **Gap** = `(demand_share − buy_share) × 100` percentage points; the bullet cites the size with the largest absolute gap.  
- **Working capital narrative:** Sums `stock × unit_cost` for slow sellers at **size_group × category**, then drills into **size** within the worst segment.

---

### Evidence chart — “Historical buy % vs true demand %” (Executive Summary)

- **Historical buy %** = `buy_qty` for that size / sum of `buy_qty` over sizes in the current slice.  
- **True demand %** = `sales_qty` for that size / sum of `sales_qty` over sizes.  
- *“True demand”* means **observed sales mix** in the selected horizon, not a forecast model.

---

### Demand Analysis — mismatch

Same share definitions as the chart above. **Gap (percentage points)** = `(demand_share − buy_share) × 100` per size.

---

### Action Queue — priority scores

Per profile `(brand, category, fit, size_group)`:

- **Missed revenue (raw):** sum of row-level `max(sold − stock, 0) × list × write-off weight` within the profile (not avg list × aggregate gap).  
- **Markdown risk (raw):** `max(stock − sold, 0) × avg list × 30%`.  
- **Mismatch severity (raw):** `|buy_units − sold_units|`.  
- Each raw column is **scaled 0–100** within the current shortlist (`normalize_0_100`).  
- **Blended score** = `0.40 × missed + 0.35 × markdown + 0.25 × mismatch`.  
- **Confidence:** High if sold_units ≥ 300; Medium if ≥ 100; else Low (separate from forecast confidence elsewhere).

---

### Optimization Engine — € projections

When you change the PO target quantity, the engine reallocates units with **largest remainder** so integer quantities match the target. **Optimal** curve uses **sales** shares; **historical** curve uses **buy** shares. Per-size € line uses:

- **Extra units** (optimal > historical): `delta_qty × list × 35%` (`INCREMENTAL_CONTRIBUTION_RATE`).  
- **Fewer units** (optimal < historical): `|delta_qty| × list × 30%` as avoided markdown risk (`MARKDOWN_RATE`).  

---

### Forecast Confidence page

Scenario bands use **per-size** `sales_qty / horizon_days × horizon_days` as a baseline; conservative/optimistic multipliers are defined in `charts.render_forecast_chart` (including a wider band when total sold units in scope is low).

---

*All € figures are **heuristic** and depend on list price, recorded sales, and stock; they are intended for relative prioritization and discussion, not statutory reporting.*
"""
    )

scoped, _cfg = render_app_shell(require_non_empty_scope=False)

if scoped is None:
    st.warning("Connect to PostgreSQL via `.streamlit/secrets.toml` to load merged buying facts.")
elif scoped.empty:
    st.info("Current filters returned no rows — widen brand/size scope or change horizon.")
else:
    st.success(f"**{len(scoped):,}** fact rows in scope — pick a page from the sidebar.")
