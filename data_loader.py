"""
Data access and business logic for the Buying Control Tower.

Merged facts are loaded via a single SQL query (join + aggregation in Postgres)
to avoid SELECT * and in-Python full outer merges.
"""
from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

REQUIRED_KEYS = ["item_no", "colour_no", "size", "barcode"]
RANK_MODES = ["Blended", "Missed Revenue", "Markdown Risk", "Mismatch Severity"]

# Shared labels for charts (charts.py uses VIZ_* constants; keep aliases for imports).
SERIES_COLORS = {
    "historical_buy_pct": "#94a3b8",
    "true_demand_pct": "#ea580c",
    "buy_share": "#64748b",
    "demand_share": "#ea580c",
    "conservative": "#cbd5e1",
    "base": "#64748b",
    "optimistic": "#94a3b8",
}

# Optimization Engine: simple rubric for € impact of rebalancing a fixed PO quantity.
MARKDOWN_RATE = 0.2
INCREMENTAL_CONTRIBUTION_RATE = 0.35  # contribution € per € list on incremental buy toward demand

# ---------------------------------------------------------------------------
# Physical column names per table (adjust if your Lions Fashion schema differs)
# ---------------------------------------------------------------------------
# Expected logical names AFTER merge match the old pandas-prefixed dataframe.
SALES_QTY_COL = os.getenv("PG_SALES_QTY_COL", "quantity")
SALES_DATE_COL = os.getenv("PG_SALES_DATE_COL", "order_date")
SALES_SEASON_COL = os.getenv("PG_SALES_SEASON_COL", "season")
SALES_BRAND_COL = os.getenv("PG_SALES_BRAND_COL", "brand")

INV_STOCK_COL = os.getenv("PG_INV_STOCK_COL", "stock")

PUR_QTY_COL = os.getenv("PG_PUR_QTY_COL", "quantity")
PUR_PRICE_COL = os.getenv("PG_PUR_PRICE_COL", "purchase_price")
PUR_SEASON_COL = os.getenv("PG_PUR_SEASON_COL", "season")

PROD_LISTPRICE_COL = os.getenv("PG_PROD_LISTPRICE_COL", "sales_listprice")
PROD_SEASON_COL = os.getenv("PG_PROD_SEASON_COL", "season")
PROD_BRAND_COL = os.getenv("PG_PROD_BRAND_COL", "brand")
PROD_ITEM_GROUP_COL = os.getenv("PG_PROD_ITEM_GROUP_COL", "item_group")
PROD_FIT_COL = os.getenv("PG_PROD_FIT_COL", "fit")
PROD_SIZE_GROUP_COL = os.getenv("PG_PROD_SIZE_GROUP_COL", "size_group")


def get_postgres_config() -> dict:
    pg = st.secrets.get("postgres", {})
    return {
        "host": pg.get("host", os.getenv("PGHOST", "localhost")),
        "port": str(pg.get("port", os.getenv("PGPORT", "5432"))),
        "database": pg.get("database", os.getenv("PGDATABASE", "postgres")),
        "user": pg.get("user", os.getenv("PGUSER", "postgres")),
        "password": pg.get("password", os.getenv("PGPASSWORD", "")),
        "schema": pg.get("schema", os.getenv("PGSCHEMA", "public")),
    }


def build_connection_url(cfg: dict) -> str:
    return f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"


def list_schema_tables(engine, schema: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = :schema_name
                ORDER BY table_name
                """
            ),
            {"schema_name": schema},
        ).fetchall()
    return [row[0] for row in rows]


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = cleaned.columns.str.strip().str.lower().str.replace(" ", "_")
    cleaned = cleaned.drop_duplicates()
    datetime_candidates = [
        col for col in cleaned.columns if "date" in col or col.endswith("_at") or "time" in col
    ]
    for col in datetime_candidates:
        cleaned[col] = pd.to_datetime(cleaned[col], errors="coerce")
    for col in cleaned.columns:
        if pd.api.types.is_numeric_dtype(cleaned[col]):
            cleaned[col] = cleaned[col].fillna(0)
        elif pd.api.types.is_datetime64_any_dtype(cleaned[col]):
            cleaned[col] = cleaned[col].fillna(pd.Timestamp("1970-01-01"))
        else:
            cleaned[col] = cleaned[col].astype("string").fillna("Unknown")
    return cleaned


def _merged_facts_sql(schema: str) -> str:
    """Single pass: union keys, aggregate sources, left-join facts (Postgres)."""
    # Identifier injection only from validated config schema (not user input in UI).
    sch = schema.replace('"', "")
    return f"""
WITH key_rows AS (
  SELECT item_no, colour_no, size, barcode FROM "{sch}"."total_sales_b2c"
  UNION
  SELECT item_no, colour_no, size, barcode FROM "{sch}"."inventory"
  UNION
  SELECT item_no, colour_no, size, barcode FROM "{sch}"."purchased"
  UNION
  SELECT item_no, colour_no, size, barcode FROM "{sch}"."products"
),
sales_agg AS (
  SELECT
    item_no, colour_no, size, barcode,
    SUM(COALESCE("{SALES_QTY_COL}"::double precision, 0)) AS sales_quantity,
    MAX("{SALES_DATE_COL}") AS sales_order_date,
    MAX(NULLIF(trim(both from "{SALES_SEASON_COL}"::text), '')) AS sales_season,
    MAX(NULLIF(trim(both from "{SALES_BRAND_COL}"::text), '')) AS sales_brand
  FROM "{sch}"."total_sales_b2c"
  GROUP BY 1, 2, 3, 4
),
inv_agg AS (
  SELECT
    item_no, colour_no, size, barcode,
    SUM(COALESCE("{INV_STOCK_COL}"::double precision, 0)) AS inv_stock
  FROM "{sch}"."inventory"
  GROUP BY 1, 2, 3, 4
),
pur_agg AS (
  SELECT
    item_no, colour_no, size, barcode,
    SUM(COALESCE("{PUR_QTY_COL}"::double precision, 0)) AS pur_quantity,
    MAX("{PUR_PRICE_COL}"::double precision) AS pur_purchase_price,
    MAX(NULLIF(trim(both from "{PUR_SEASON_COL}"::text), '')) AS pur_season
  FROM "{sch}"."purchased"
  GROUP BY 1, 2, 3, 4
),
prod_dedup AS (
  SELECT DISTINCT ON (item_no, colour_no, size, barcode)
    item_no, colour_no, size, barcode,
    COALESCE("{PROD_LISTPRICE_COL}"::double precision, 0) AS prod_sales_listprice,
    "{PROD_SEASON_COL}" AS prod_season,
    "{PROD_BRAND_COL}" AS prod_brand,
    "{PROD_ITEM_GROUP_COL}" AS prod_item_group,
    "{PROD_FIT_COL}" AS prod_fit,
    "{PROD_SIZE_GROUP_COL}" AS prod_size_group
  FROM "{sch}"."products"
  ORDER BY item_no, colour_no, size, barcode, COALESCE("{PROD_LISTPRICE_COL}"::double precision, 0) DESC NULLS LAST
)
SELECT
  k.item_no,
  k.colour_no,
  k.size,
  k.barcode,
  s.sales_quantity,
  s.sales_order_date,
  s.sales_season,
  s.sales_brand,
  i.inv_stock,
  p.pur_quantity,
  p.pur_purchase_price,
  p.pur_season,
  pr.prod_sales_listprice,
  pr.prod_season,
  pr.prod_brand,
  pr.prod_item_group,
  pr.prod_fit,
  pr.prod_size_group
FROM key_rows k
LEFT JOIN sales_agg s
  ON s.item_no = k.item_no AND s.colour_no = k.colour_no
  AND s.size = k.size AND s.barcode = k.barcode
LEFT JOIN inv_agg i
  ON i.item_no = k.item_no AND i.colour_no = k.colour_no
  AND i.size = k.size AND i.barcode = k.barcode
LEFT JOIN pur_agg p
  ON p.item_no = k.item_no AND p.colour_no = k.colour_no
  AND p.size = k.size AND p.barcode = k.barcode
LEFT JOIN prod_dedup pr
  ON pr.item_no = k.item_no AND pr.colour_no = k.colour_no
  AND pr.size = k.size AND pr.barcode = k.barcode
"""


@st.cache_data(show_spinner="Loading merged facts from PostgreSQL...")
def load_and_merge_data() -> pd.DataFrame:
    cfg = get_postgres_config()
    schema = cfg["schema"]
    engine = create_engine(build_connection_url(cfg))
    sql = _merged_facts_sql(schema)
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    cleaned = _clean_dataframe(df)
    missing = [key for key in REQUIRED_KEYS if key not in cleaned.columns]
    if missing:
        raise ValueError(f"Merged query result missing required key columns: {missing}.")
    return cleaned


def _numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _text(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series("Unknown", index=df.index, dtype="string")
    return df[col].astype("string").fillna("Unknown")


def with_common_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sales_qty"] = _numeric(out, "sales_quantity")
    out["buy_qty"] = _numeric(out, "pur_quantity")
    out["stock_qty"] = _numeric(out, "inv_stock")
    out["list_price"] = _numeric(out, "prod_sales_listprice")
    out["unit_cost"] = _numeric(out, "pur_purchase_price").where(
        _numeric(out, "pur_purchase_price") > 0, _numeric(out, "prod_sales_listprice") * 0.5
    )
    out["order_date"] = pd.to_datetime(out.get("sales_order_date"), errors="coerce")
    out["season"] = _text(out, "prod_season")
    out["season"] = out["season"].where(out["season"] != "Unknown", _text(out, "sales_season"))
    out["season"] = out["season"].where(out["season"] != "Unknown", _text(out, "pur_season"))
    out["brand"] = _text(out, "prod_brand")
    out["brand"] = out["brand"].where(out["brand"] != "Unknown", _text(out, "sales_brand"))
    out["category"] = _text(out, "prod_item_group")
    out["fit"] = _text(out, "prod_fit")
    out["size_group"] = _text(out, "prod_size_group")
    out["style"] = _text(out, "item_no")
    return out


def normalize_0_100(values: pd.Series) -> pd.Series:
    minimum = values.min()
    maximum = values.max()
    if maximum == minimum:
        return pd.Series(50.0, index=values.index)
    return ((values - minimum) / (maximum - minimum) * 100).clip(0, 100)


def largest_remainder_alloc(shares: pd.Series, target_qty: int, sales_units: pd.Series) -> pd.Series:
    raw = shares * target_qty
    base = raw.apply(int)
    remaining = int(target_qty - base.sum())
    fractions = (raw - base).sort_values(ascending=False)
    if remaining > 0:
        tie_df = pd.DataFrame({"fraction": fractions, "sales_units": sales_units.reindex(fractions.index)})
        tie_df = tie_df.sort_values(["fraction", "sales_units"], ascending=[False, False])
        for idx in tie_df.index[:remaining]:
            base.loc[idx] += 1
    return base.astype(int)


def confidence_label(sold_units: float, coverage: float, freshness_days: int, fallback_used: bool) -> str:
    if sold_units >= 300 and coverage >= 0.8 and freshness_days <= 14 and not fallback_used:
        return "High"
    if sold_units < 100 or coverage < 0.5 or fallback_used:
        return "Low"
    return "Medium"


def compute_kpis(df: pd.DataFrame, horizon_days: int) -> tuple[float, float, float]:
    daily_rate = (df["sales_qty"].sum() / max(horizon_days, 1)) if not df.empty else 0
    expected_units = max(daily_rate * horizon_days, 0)
    lost_revenue = float(((expected_units - df["stock_qty"]).clip(lower=0) * df["list_price"]).sum())
    overstock_units = (df["stock_qty"] - df["sales_qty"]).clip(lower=0)
    margin_eroded = float((overstock_units * df["list_price"] * 0.2).sum())
    sell_through = df["sales_qty"] / df["stock_qty"].replace(0, pd.NA)
    capital_at_risk = float(
        (df["stock_qty"] * df["unit_cost"] * (sell_through.fillna(0) < 0.3).astype(float)).sum()
    )
    return lost_revenue, margin_eroded, capital_at_risk


def _row_level_risk_frames(df: pd.DataFrame) -> pd.DataFrame:
    """Per-row proxies for executive narrative (attribution, not full KPI reconciliation)."""
    if df.empty:
        return df
    out = df.copy()
    # Understock value proxy: demand not covered by on-hand at row grain
    out["_understock_value"] = (out["sales_qty"] - out["stock_qty"]).clip(lower=0) * out["list_price"]
    overstock = (out["stock_qty"] - out["sales_qty"]).clip(lower=0)
    out["_margin_eroded_row"] = overstock * out["list_price"] * 0.2
    st_ratio = out["sales_qty"] / out["stock_qty"].replace(0, pd.NA)
    slow = (st_ratio.fillna(0) < 0.3).astype(float)
    out["_wc_risk_row"] = out["stock_qty"] * out["unit_cost"] * slow
    return out


def build_executive_narrative(df: pd.DataFrame, horizon_days: int) -> tuple[list[str], dict]:
    """
    Return markdown-friendly headline bullets and KPI numbers for the executive view.
    """
    lost_rev, margin_er, wc = compute_kpis(df, horizon_days)
    bullets: list[str] = []

    if df.empty:
        return ["No rows in the current scope — widen filters or change horizon."], {
            "lost_revenue": lost_rev,
            "margin_eroded": margin_er,
            "working_capital": wc,
        }

    enriched = _row_level_risk_frames(df)

    # Working capital at risk: top size_group x category
    seg_wc = (
        enriched.groupby(["size_group", "category"], dropna=False)["_wc_risk_row"]
        .sum()
        .sort_values(ascending=False)
    )
    if not seg_wc.empty and seg_wc.iloc[0] > 0:
        (sg, cat), val = seg_wc.index[0], float(seg_wc.iloc[0])
        top_size = (
            enriched[(enriched["size_group"] == sg) & (enriched["category"] == cat)]
            .groupby("size", dropna=False)["_wc_risk_row"]
            .sum()
            .sort_values(ascending=False)
        )
        size_hint = f"**{top_size.index[0]}**" if not top_size.empty and top_size.iloc[0] > 0 else "**multiple sizes**"
        bullets.append(
            f"### Where capital is stuck\n\nAbout **€{val:,.0f}** of working capital sits in excess inventory with sell-through "
            f"under 30%, concentrated in **{sg}** / **{cat}** — most visible on size {size_hint}."
        )

    # Understock / demand not served proxy: top size
    by_size_lr = enriched.groupby("size", dropna=False)["_understock_value"].sum().sort_values(ascending=False)
    if not by_size_lr.empty and by_size_lr.iloc[0] > 0:
        sz, val = str(by_size_lr.index[0]), float(by_size_lr.iloc[0])
        bullets.append(
            f"### Demand not covered by stock\n\nThe largest **understock** signal (demand above on-hand × list price) is on "
            f"**size {sz}** (~**€{val:,.0f}** in this scope)."
        )

    # Markdown / overstock: top size
    by_size_md = enriched.groupby("size", dropna=False)["_margin_eroded_row"].sum().sort_values(ascending=False)
    if not by_size_md.empty and by_size_md.iloc[0] > 0:
        sz, val = str(by_size_md.index[0]), float(by_size_md.iloc[0])
        bullets.append(
            f"### Markdown pressure\n\nOver-buy vs demand hurts most on **size {sz}** "
            f"(~**€{val:,.0f}** margin erosion at a 20% markdown assumption)."
        )

    # Buy vs demand gap (percentage points) at scope level
    by_sz = df.groupby("size", dropna=False)[["buy_qty", "sales_qty"]].sum()
    if not by_sz.empty and by_sz["buy_qty"].sum() > 0 and by_sz["sales_qty"].sum() > 0:
        by_sz = by_sz.copy()
        by_sz["buy_share"] = by_sz["buy_qty"] / by_sz["buy_qty"].sum()
        by_sz["demand_share"] = by_sz["sales_qty"] / by_sz["sales_qty"].sum()
        by_sz["gap_pp"] = (by_sz["demand_share"] - by_sz["buy_share"]) * 100
        worst = by_sz["gap_pp"].abs().idxmax()
        gap = float(by_sz.loc[worst, "gap_pp"])
        bullets.append(
            f"### Buy curve vs demand\n\nThe largest mismatch is **size {worst}** "
            f"({gap:+.1f} percentage points vs demand share)."
        )

    if not bullets:
        bullets.append("### Snapshot\n\nNo major anomalies detected in this scope — KPIs look relatively balanced.")

    return bullets, {"lost_revenue": lost_rev, "margin_eroded": margin_er, "working_capital": wc}


def _resolve_profile_local(
    df: pd.DataFrame, brand: str, category: str, fit: str, size_group: str, style: str
) -> tuple[pd.DataFrame, float, bool]:
    """
    Row-level scope for a buying profile, including fallback when sample is thin.
    Returns (local_df, sold_units, fallback_used).
    """
    local = df[
        (df["brand"] == brand) & (df["category"] == category) & (df["fit"] == fit) & (df["size_group"] == size_group)
    ].copy()
    if style != "All":
        local = local[local["style"] == style]
    sold_units = float(local["sales_qty"].sum())
    fallback_used = sold_units < 100
    if fallback_used:
        broad = df[(df["brand"] == brand) & (df["category"] == category) & (df["fit"] == fit)].copy()
        in_group = broad[broad["size_group"] == size_group]
        local = in_group if not in_group.empty else broad[broad["size"].isin(local["size"].unique())]
        if local.empty:
            local = df[df["size_group"] == size_group].copy()
    return local, sold_units, fallback_used


def profile_recommendation(
    df: pd.DataFrame, brand: str, category: str, fit: str, size_group: str, target_qty: int, style: str
) -> tuple[pd.DataFrame, dict]:
    local, sold_units, fallback_used = _resolve_profile_local(df, brand, category, fit, size_group, style)
    size_curve = local.groupby("size", dropna=False)["sales_qty"].sum().reset_index()
    if size_curve.empty:
        return pd.DataFrame(), {"fallback_used": fallback_used, "sold_units": sold_units, "confidence": "Low"}
    total_sales = max(size_curve["sales_qty"].sum(), 1)
    size_curve["recommended_share"] = size_curve["sales_qty"] / total_sales
    alloc = largest_remainder_alloc(size_curve["recommended_share"], target_qty, size_curve["sales_qty"])
    size_curve["recommended_qty"] = alloc.values
    coverage = len(local[REQUIRED_KEYS + ["sales_qty"]].dropna()) / max(len(local), 1)
    latest = pd.to_datetime(local["order_date"], errors="coerce").max()
    freshness = (pd.Timestamp.today() - latest).days if pd.notna(latest) else 999
    confidence = confidence_label(sold_units, coverage, freshness, fallback_used)
    meta = {
        "fallback_used": fallback_used,
        "sold_units": sold_units,
        "coverage": coverage,
        "freshness_days": freshness,
        "confidence": confidence,
    }
    return size_curve.sort_values("size"), meta


def build_optimization_detail(
    df: pd.DataFrame,
    brand: str,
    category: str,
    fit: str,
    size_group: str,
    target_qty: int,
    style: str,
) -> tuple[pd.DataFrame, dict]:
    """
    Full optimization story for UI: historical buy curve vs demand-optimal curve at ``target_qty``,
    variance in percentage points, and a simple € projection per size from moving units.

    Historical PO shape uses observed ``buy_qty`` shares; optimal uses demand (``sales_qty``) shares.
    Both are integer-allocated with largest remainder to the same target so quantities are comparable.

    Margin heuristic (transparent for the Head of Buying):
    - Extra units vs historical curve → incremental contribution at INCREMENTAL_CONTRIBUTION_RATE × list price.
    - Fewer units vs historical curve → avoided markdown risk at MARKDOWN_RATE × list price.
    """
    local, sold_units, fallback_used = _resolve_profile_local(df, brand, category, fit, size_group, style)
    agg = (
        local.groupby("size", dropna=False)
        .agg(sales_qty=("sales_qty", "sum"), buy_qty=("buy_qty", "sum"), avg_list_price=("list_price", "mean"))
        .reset_index()
    )
    if agg.empty:
        return pd.DataFrame(), {
            "fallback_used": fallback_used,
            "sold_units": sold_units,
            "profile_label": f"{brand} · {category} · {fit} · {size_group}",
            "total_projected_margin_eur": 0.0,
            "confidence": "Low",
        }

    total_sales = max(agg["sales_qty"].sum(), 1.0)
    total_buy = float(agg["buy_qty"].sum())
    n_sizes = max(len(agg), 1)
    agg["demand_share"] = agg["sales_qty"] / total_sales
    if total_buy > 0:
        agg["historical_buy_share"] = agg["buy_qty"] / total_buy
    else:
        agg["historical_buy_share"] = 1.0 / n_sizes

    qty_optimal = largest_remainder_alloc(agg.set_index("size")["demand_share"], target_qty, agg.set_index("size")["sales_qty"])
    qty_historical = largest_remainder_alloc(
        agg.set_index("size")["historical_buy_share"], target_qty, agg.set_index("size")["buy_qty"]
    )

    agg = agg.set_index("size")
    agg["qty_optimal_at_target"] = qty_optimal.reindex(agg.index).fillna(0).astype(int)
    agg["qty_historical_at_target"] = qty_historical.reindex(agg.index).fillna(0).astype(int)
    agg["delta_qty"] = agg["qty_optimal_at_target"] - agg["qty_historical_at_target"]
    agg["alloc_share_optimal"] = agg["qty_optimal_at_target"] / max(target_qty, 1)
    agg["alloc_share_historical"] = agg["qty_historical_at_target"] / max(target_qty, 1)
    agg["variance_pp"] = (agg["alloc_share_optimal"] - agg["alloc_share_historical"]) * 100.0
    agg["curve_gap_pp"] = (agg["demand_share"] - agg["historical_buy_share"]) * 100.0

    price = agg["avg_list_price"].fillna(0.0)
    inc = (agg["delta_qty"] > 0) * agg["delta_qty"] * price * INCREMENTAL_CONTRIBUTION_RATE
    dec = (agg["delta_qty"] < 0) * (-agg["delta_qty"]) * price * MARKDOWN_RATE
    agg["projected_margin_eur"] = (inc + dec).round(2)
    agg = agg.reset_index()

    coverage = len(local[REQUIRED_KEYS + ["sales_qty"]].dropna()) / max(len(local), 1)
    latest = pd.to_datetime(local["order_date"], errors="coerce").max()
    freshness = (pd.Timestamp.today() - latest).days if pd.notna(latest) else 999
    confidence = confidence_label(sold_units, coverage, freshness, fallback_used)

    detail = agg.sort_values("size").rename(
        columns={
            "size": "Size",
            "historical_buy_share": "hist_buy_share",
            "demand_share": "optimal_demand_share",
            "alloc_share_historical": "po_share_historical_curve",
            "alloc_share_optimal": "po_share_optimal_curve",
        }
    )

    total_proj = float(detail["projected_margin_eur"].sum())
    meta = {
        "fallback_used": fallback_used,
        "sold_units": sold_units,
        "coverage": coverage,
        "freshness_days": freshness,
        "confidence": confidence,
        "profile_label": f"{brand} · {category} · {fit} · {size_group}",
        "target_qty": target_qty,
        "total_projected_margin_eur": total_proj,
    }
    if style != "All":
        meta["profile_label"] += f" · style {style}"
    return detail, meta
