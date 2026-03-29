"""
Data access and business logic for the Buying Control Tower.

Merged facts are loaded via a single SQL query (join + aggregation in Postgres)
to avoid SELECT * and in-Python full outer merges.
"""
from __future__ import annotations

import os
import re

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

REQUIRED_KEYS = ["item_no", "colour_no", "size", "barcode"]
RANK_MODES = ["Blended", "Missed Revenue", "Markdown Risk", "Mismatch Severity"]

# Shared labels for charts — slate neutrals + deep maroon accent (Power BI–style discipline).
VIZ_ACCENT = "#991b1b"
SERIES_COLORS = {
    "historical_buy_pct": "#94a3b8",
    "true_demand_pct": "#991b1b",
    "buy_share": "#64748b",
    "demand_share": "#991b1b",
    "conservative": "#cbd5e1",
    "base": "#991b1b",
    "optimistic": "#94a3b8",
}

# Optimization Engine: simple rubric for € impact of rebalancing a fixed PO quantity.
MARKDOWN_RATE = 0.3
INCREMENTAL_CONTRIBUTION_RATE = 0.35  # contribution € per € list on incremental buy toward demand

# Missed revenue: SKUs with realized gross margin below this floor are treated as "written off" (no missed revenue).
MARGIN_WRITE_OFF_FLOOR = float(os.getenv("MARGIN_WRITE_OFF_FLOOR", "0.60"))

# SKU-rows with unit net revenue (ex VAT) at or below this are treated as intentional free / giveaway / comp:
# margin write-off does not apply (missed_revenue_weight stays 1 for those rows). Quantities and € still flow
# into demand, totals, and other KPIs unchanged.
FREE_OR_GIVEAWAY_UNIT_REVENUE_MAX_EUR = float(os.getenv("FREE_OR_GIVEAWAY_UNIT_REVENUE_MAX_EUR", "0.01"))

# Stock-out missed revenue only: zero weight when as-of is more than this many weeks after first sale (per SKU in merge).
STOCKOUT_WRITE_OFF_WEEKS_AFTER_FIRST_SALE = float(os.getenv("STOCKOUT_WRITE_OFF_WEEKS_AFTER_FIRST_SALE", "8"))

# Products list price (`sales_listprice`) is incl. 21% NL VAT; sales table amounts are ex VAT — divide for like-for-like.
NL_VAT_DIVISOR = float(os.getenv("NL_VAT_DIVISOR", "1.21"))

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

# Net sales for ASP / write-off: SUM ex-VAT turnover per SKU from `total_sales_b2c` (see `_build_sales_revenue_agg_sql`).
# Implied discount vs ticket is computed in Python: qty × (list_incl_vat / NL_VAT_DIVISOR) − actual ex-VAT revenue.
# Override: set PG_SALES_REVENUE_COL, or PG_SALES_EX_VAT_AMOUNT_COL in the environment; otherwise the first matching
# name in _SALES_AMOUNT_COL_AUTODETECT_ORDER is used (see `load_and_merge_data`).
PG_SALES_REVENUE_COL = os.getenv("PG_SALES_REVENUE_COL", "").strip()

# When neither PG_SALES_REVENUE_COL nor PG_SALES_EX_VAT_AMOUNT_COL is set in the environment, pick the first
# column on total_sales_b2c that matches these names (order = preference).
_SALES_AMOUNT_COL_AUTODETECT_ORDER = (
    "amount_lcy_discount",
    "amount_lcy",
    "turnover_lcy",
    "amount",
    "sales_amount",
    "line_amount",
    "line_amount_lcy",
    "net_amount",
    "amount_excl_vat",
    "amount_ex_vat",
    "turnover",
    "revenue",
    "sales_value",
    "total_amount",
    "sales_amount_lcy",
)


def _validated_identifier(name: str, label: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", str(name)):
        raise ValueError(f"Invalid {label} (use letters, digits, underscore).")
    return str(name)


def _fetch_table_columns_lower_map(conn, schema: str, table: str) -> dict[str, str]:
    """Map lowercase column name -> exact identifier as stored (for quoted SQL)."""
    rows = conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema_name AND table_name = :table_name
            """
        ),
        {"schema_name": schema, "table_name": table},
    ).fetchall()
    return {row[0].lower(): row[0] for row in rows}


def _build_sales_revenue_agg_sql(sales_cols: dict[str, str], schema: str) -> str:
    """
    SUM one revenue/amount column per SKU. Uses PG_SALES_REVENUE_COL if set; else PG_SALES_EX_VAT_AMOUNT_COL
    if set in the environment (must exist); else first matching name in _SALES_AMOUNT_COL_AUTODETECT_ORDER.
    """
    fragment = 'SUM(COALESCE("{}"::double precision, 0)) AS sales_revenue_agg'
    qual = f'{schema}.total_sales_b2c'

    def must_have(col: str, label: str) -> str:
        key = col.lower()
        if key not in sales_cols:
            found = ", ".join(sorted(sales_cols.values())) or "(no columns)"
            raise ValueError(
                f'{label}="{col}" is not a column on {qual}. '
                f"Found: {found}. Set PG_SALES_EX_VAT_AMOUNT_COL or PG_SALES_REVENUE_COL to a real column name."
            )
        return sales_cols[key]

    if PG_SALES_REVENUE_COL:
        quoted = must_have(_validated_identifier(PG_SALES_REVENUE_COL, "PG_SALES_REVENUE_COL"), "PG_SALES_REVENUE_COL")
        return fragment.format(quoted)

    if "PG_SALES_EX_VAT_AMOUNT_COL" in os.environ:
        raw = os.environ["PG_SALES_EX_VAT_AMOUNT_COL"]
        quoted = must_have(_validated_identifier(raw, "PG_SALES_EX_VAT_AMOUNT_COL"), "PG_SALES_EX_VAT_AMOUNT_COL")
        return fragment.format(quoted)

    for cand in _SALES_AMOUNT_COL_AUTODETECT_ORDER:
        if cand in sales_cols:
            return fragment.format(sales_cols[cand])

    found = ", ".join(sorted(sales_cols.values())) or "(no columns)"
    raise ValueError(
        f"Could not infer a sales amount column on {qual} (tried "
        + ", ".join(_SALES_AMOUNT_COL_AUTODETECT_ORDER)
        + f"). Columns present: {found}. Set PG_SALES_EX_VAT_AMOUNT_COL or PG_SALES_REVENUE_COL."
    )


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


def _merged_facts_sql(schema: str, sales_revenue_agg_sql: str) -> str:
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
    {sales_revenue_agg_sql},
    MIN("{SALES_DATE_COL}") AS sales_first_order_date,
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
  s.sales_revenue_agg,
  s.sales_first_order_date,
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
    with engine.connect() as conn:
        sales_cols = _fetch_table_columns_lower_map(conn, schema, "total_sales_b2c")
        revenue_agg = _build_sales_revenue_agg_sql(sales_cols, schema)
        sql = _merged_facts_sql(schema, revenue_agg)
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
    # Ticket / list from products is incl. VAT; compare to ex-VAT sales using NL divisor.
    out["list_price_ex_vat"] = out["list_price"] / NL_VAT_DIVISOR
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
    out["sales_revenue"] = _numeric(out, "sales_revenue_agg")
    # Implied markdown/discount € = ticket value ex VAT − actual ex-VAT revenue (no discount column).
    out["theoretical_revenue_ex_vat"] = out["sales_qty"] * out["list_price_ex_vat"]
    out["implied_discount_eur"] = (out["theoretical_revenue_ex_vat"] - out["sales_revenue"]).clip(lower=0)
    theo = out["theoretical_revenue_ex_vat"].replace(0, pd.NA)
    out["implied_discount_pct"] = (out["implied_discount_eur"] / theo).fillna(0.0).clip(0.0, 1.0)
    return out


def missed_revenue_weight(df: pd.DataFrame) -> pd.Series:
    """
    Per-SKU weight in [0, 1] for missed-revenue style metrics.

    Uses gross margin = (ASP − unit_cost) / ASP. ASP is ex-VAT net sales / units only when the row has
    paid-through net revenue above the giveaway threshold; otherwise ticket ex VAT (`list_price_ex_vat`).

    Free / comp / giveaway lines (`sales_revenue` ≤ 0 or unit revenue ≤ FREE_OR_GIVEAWAY_UNIT_REVENUE_MAX_EUR)
    are assumed intentional: they never receive margin-based write-off (weight stays 1), while `sales_qty`
    and `sales_revenue` still contribute everywhere else.

    Weight is 0 when margin is below MARGIN_WRITE_OFF_FLOOR (default 60%) on **non-giveaway** rows with
    a realized price signal: treated as written off / too heavily discounted for recoverable missed revenue.
    """
    lp_ex = _numeric(df, "list_price_ex_vat") if "list_price_ex_vat" in df.columns else _numeric(df, "list_price") / NL_VAT_DIVISOR
    cost = _numeric(df, "unit_cost")
    qty = _numeric(df, "sales_qty")
    rev = _numeric(df, "sales_revenue")
    use_realized = (qty > 0) & (rev > 0)
    unit_rev = rev / qty.replace(0, pd.NA)
    giveaway = (qty > 0) & ((rev <= 0) | (unit_rev <= FREE_OR_GIVEAWAY_UNIT_REVENUE_MAX_EUR))
    margin_signal = use_realized & ~giveaway
    asp = lp_ex.copy()
    asp = asp.where(~margin_signal, rev / qty)
    margin = (asp - cost) / asp.replace(0, pd.NA)
    w = (margin >= MARGIN_WRITE_OFF_FLOOR).astype(float)
    w = w.mask(asp <= 0, 0.0)
    w = w.mask((asp > 0) & margin.isna(), 1.0)
    # No margin write-off without a paid-through price signal, or on intentional free lines.
    w = w.mask(~margin_signal, 1.0)
    return w.clip(0.0, 1.0)


def margin_below_writeoff_floor_for_stockout(df: pd.DataFrame) -> pd.Series:
    """
    Per-row {0, 1}: 1 only where there is a **realized** (non-giveaway) price signal and gross margin is
    **strictly below** ``MARGIN_WRITE_OFF_FLOOR`` (and ASP > 0 with finite margin). Used to gate
    stock-out missed revenue so the demand-vs-stock horizon applies **only** in that margin-distress regime.
    Giveaway lines and list-only / no paid-through rows → 0.
    """
    lp_ex = _numeric(df, "list_price_ex_vat") if "list_price_ex_vat" in df.columns else _numeric(df, "list_price") / NL_VAT_DIVISOR
    cost = _numeric(df, "unit_cost")
    qty = _numeric(df, "sales_qty")
    rev = _numeric(df, "sales_revenue")
    use_realized = (qty > 0) & (rev > 0)
    unit_rev = rev / qty.replace(0, pd.NA)
    giveaway = (qty > 0) & ((rev <= 0) | (unit_rev <= FREE_OR_GIVEAWAY_UNIT_REVENUE_MAX_EUR))
    margin_signal = use_realized & ~giveaway
    asp = lp_ex.copy()
    asp = asp.where(~margin_signal, rev / qty)
    margin = (asp - cost) / asp.replace(0, pd.NA)
    distressed = margin_signal & (asp > 0) & margin.notna() & (margin < MARGIN_WRITE_OFF_FLOOR)
    return distressed.astype(float).clip(0.0, 1.0)


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


def compute_kpis(df: pd.DataFrame, horizon_days: int, as_of: object | None = None) -> tuple[float, float, float, dict]:
    """
    Headline **Total Lost Revenue** = **margin eroded** (overstock × list × ``MARKDOWN_RATE``)
    + **working capital at risk** (slow sell-through rule). Stock-out stats are returned separately
    for the Executive stock-outs panel only (see ``stockout_missed_revenue_stats``).
    Returns ``(lost_revenue, margin_eroded, capital_at_risk, stockout_breakdown)`` where
    ``stockout_breakdown`` is the full dict from ``stockout_missed_revenue_stats`` for UI merge.
    """
    stockout = stockout_missed_revenue_stats(df, horizon_days, as_of)
    if df.empty:
        return 0.0, 0.0, 0.0, stockout
    overstock_units = (df["stock_qty"] - df["sales_qty"]).clip(lower=0)
    margin_eroded = float((overstock_units * df["list_price"] * MARKDOWN_RATE).sum())
    sell_through = df["sales_qty"] / df["stock_qty"].replace(0, pd.NA)
    capital_at_risk = float(
        (df["stock_qty"] * df["unit_cost"] * (sell_through.fillna(0) < 0.3).astype(float)).sum()
    )
    lost_revenue = margin_eroded + capital_at_risk
    return lost_revenue, margin_eroded, capital_at_risk, stockout


def _stockout_first_sale_age_weight(df: pd.DataFrame, as_of: object | None) -> pd.Series:
    """
    Per-row multiplier in {0, 1} for stock-out € only: 0 when ``as_of`` is after
    ``sales_first_order_date`` + STOCKOUT_WRITE_OFF_WEEKS_AFTER_FIRST_SALE weeks.

    Missing ``sales_first_order_date`` or missing ``as_of`` → all 1.0 (no age cut-off).
    """
    if as_of is None or df.empty:
        return pd.Series(1.0, index=df.index, dtype=float)
    as_ts = pd.Timestamp(as_of).normalize()
    if "sales_first_order_date" not in df.columns:
        return pd.Series(1.0, index=df.index, dtype=float)
    first = pd.to_datetime(df["sales_first_order_date"], errors="coerce")
    delta = pd.Timedelta(weeks=float(STOCKOUT_WRITE_OFF_WEEKS_AFTER_FIRST_SALE))
    cutoff = first + delta
    stale = first.notna() & (as_ts > cutoff)
    return (~stale).astype(float)


def stockout_missed_revenue_stats(df: pd.DataFrame, horizon_days: int, as_of: object | None = None) -> dict[str, float | int]:
    """
    Stock-out / understock missed-revenue breakdown for the Executive Summary (informational only).

    Same scope-level **horizon** (``daily_rate × horizon_days`` vs ``stock_qty``); the € gap applies
    **only** where gross margin is **strictly below** ``MARGIN_WRITE_OFF_FLOOR`` with a non-giveaway
    paid-through price signal, then **first-sale age** weight.

    **Rows with no on-hand stock but positive sales are excluded** from ``stockout_missed_revenue_eur``;
    their counterfactual € is still reported in ``stockout_zero_inventory_missed_eur`` for diagnostics.
    """
    if df.empty:
        return {
            "stockout_missed_revenue_eur": 0.0,
            "stockout_skus_with_gap": 0,
            "stockout_zero_inventory_missed_eur": 0.0,
            "stockout_expected_demand_units": 0.0,
            "stockout_skus_age_written_off": 0,
        }
    daily_rate = df["sales_qty"].sum() / max(horizon_days, 1)
    expected_units = max(daily_rate * horizon_days, 0)
    w_margin_distress = margin_below_writeoff_floor_for_stockout(df)
    w_age = _stockout_first_sale_age_weight(df, as_of)
    w_stockout = w_margin_distress * w_age
    gap_units = (expected_units - df["stock_qty"]).clip(lower=0)
    weighted_eur = gap_units * df["list_price"] * w_stockout
    zero_oh = (df["stock_qty"] <= 0) & (df["sales_qty"] > 0)
    eur_reported = weighted_eur.where(~zero_oh, 0.0)
    skus_age_off = int((w_age < 0.5).sum())
    return {
        "stockout_missed_revenue_eur": float(eur_reported.sum()),
        "stockout_skus_with_gap": int(((gap_units > 0) & (w_stockout > 0) & (~zero_oh)).sum()),
        "stockout_zero_inventory_missed_eur": float(weighted_eur.where(zero_oh, 0.0).sum()),
        "stockout_expected_demand_units": float(expected_units),
        "stockout_skus_age_written_off": skus_age_off,
    }


def _row_level_risk_frames(df: pd.DataFrame) -> pd.DataFrame:
    """Per-row proxies for executive narrative (attribution, not full KPI reconciliation)."""
    if df.empty:
        return df
    out = df.copy()
    # Understock value proxy: demand not covered by on-hand at row grain (written-off SKUs excluded)
    w = missed_revenue_weight(out)
    out["_understock_value"] = (out["sales_qty"] - out["stock_qty"]).clip(lower=0) * out["list_price"] * w
    overstock = (out["stock_qty"] - out["sales_qty"]).clip(lower=0)
    out["_margin_eroded_row"] = overstock * out["list_price"] * MARKDOWN_RATE
    st_ratio = out["sales_qty"] / out["stock_qty"].replace(0, pd.NA)
    slow = (st_ratio.fillna(0) < 0.3).astype(float)
    out["_wc_risk_row"] = out["stock_qty"] * out["unit_cost"] * slow
    return out


def build_executive_narrative(df: pd.DataFrame, horizon_days: int, as_of: object | None = None) -> tuple[list[str], dict]:
    """
    Return markdown-friendly headline bullets and KPI numbers for the executive view.
    """
    lost_rev, margin_er, wc, stockout = compute_kpis(df, horizon_days, as_of)
    bullets: list[str] = []

    if df.empty:
        base = {
            "lost_revenue": lost_rev,
            "margin_eroded": margin_er,
            "working_capital": wc,
        }
        base.update(stockout)
        return ["No rows in the current scope — widen filters or change horizon."], base

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
            f"(~**€{val:,.0f}** margin erosion at a {MARKDOWN_RATE:.0%} markdown assumption)."
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

    base_kpis = {
        "lost_revenue": lost_rev,
        "margin_eroded": margin_er,
        "working_capital": wc,
    }
    base_kpis.update(stockout)
    return bullets, base_kpis


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
