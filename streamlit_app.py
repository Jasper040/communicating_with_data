import os
from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


st.set_page_config(page_title="Communicating With Data", layout="wide")

REQUIRED_KEYS = ["item_no", "colour_no", "size", "barcode"]
RANK_MODES = ["Blended", "Missed Revenue", "Markdown Risk", "Mismatch Severity"]
SERIES_COLORS = {
    "historical_buy_pct": "#1d4ed8",
    "true_demand_pct": "#ea580c",
    "buy_share": "#1d4ed8",
    "demand_share": "#ea580c",
    "conservative": "#7c3aed",
    "base": "#0f766e",
    "optimistic": "#d97706",
}


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


def _require_keys(df: pd.DataFrame, source_name: str) -> None:
    missing = [key for key in REQUIRED_KEYS if key not in df.columns]
    if missing:
        raise ValueError(f"{source_name} is missing required key columns: {missing}.")


def _prefix_non_keys(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    rename_map = {col: f"{prefix}{col}" for col in df.columns if col not in REQUIRED_KEYS}
    return df.rename(columns=rename_map)


def _numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _text(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series("Unknown", index=df.index, dtype="string")
    return df[col].astype("string").fillna("Unknown")


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


@st.cache_data(show_spinner="Loading data from PostgreSQL...")
def load_and_merge_data() -> pd.DataFrame:
    cfg = get_postgres_config()
    schema = cfg["schema"]
    engine = create_engine(build_connection_url(cfg))
    prefixes = {
        "total_sales_b2c": "sales_",
        "inventory": "inv_",
        "purchased": "pur_",
        "products": "prod_",
    }
    frames = {}
    with engine.connect() as conn:
        for table_name, prefix in prefixes.items():
            df = pd.read_sql(f'SELECT * FROM "{schema}"."{table_name}"', conn)
            cleaned = _clean_dataframe(df)
            _require_keys(cleaned, table_name)
            frames[table_name] = _prefix_non_keys(cleaned, prefix)
    merged = frames["total_sales_b2c"]
    for source in ["inventory", "purchased", "products"]:
        merged = merged.merge(frames[source], on=REQUIRED_KEYS, how="outer")
    return _clean_dataframe(merged)


def _normalize_0_100(values: pd.Series) -> pd.Series:
    minimum = values.min()
    maximum = values.max()
    if maximum == minimum:
        return pd.Series(50.0, index=values.index)
    return ((values - minimum) / (maximum - minimum) * 100).clip(0, 100)


def _largest_remainder_alloc(shares: pd.Series, target_qty: int, sales_units: pd.Series) -> pd.Series:
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


def _with_common_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sales_qty"] = _numeric(out, "sales_quantity")
    out["buy_qty"] = _numeric(out, "pur_quantity")
    out["stock_qty"] = _numeric(out, "inv_stock")
    out["list_price"] = _numeric(out, "prod_sales_listprice")
    out["unit_cost"] = _numeric(out, "pur_purchase_price").where(_numeric(out, "pur_purchase_price") > 0, _numeric(out, "prod_sales_listprice") * 0.5)
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


def apply_global_controls(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    normalized = _with_common_metrics(df)
    all_brands = sorted(normalized["brand"].dropna().unique().tolist())
    all_size_groups = sorted(normalized["size_group"].dropna().unique().tolist())
    all_seasons = sorted(normalized["season"].dropna().unique().tolist(), reverse=True)
    order_dates = pd.to_datetime(normalized["order_date"], errors="coerce")
    default_as_of_ts = order_dates.max() if order_dates.notna().any() else pd.Timestamp.today()
    default_as_of = default_as_of_ts.date()
    with st.sidebar.expander("Global Filters", expanded=True):
        selected_brands = st.multiselect("Brands", all_brands, default=all_brands)
        selected_size_groups = st.multiselect("Size Groups", all_size_groups, default=all_size_groups)
        horizon = st.selectbox("Horizon", ["Season", "4 weeks", "PO"])
        as_of = st.date_input("As-of date", value=default_as_of)
        po_start, po_end, season = None, None, None
        if horizon == "PO":
            po_start = st.date_input("PO start date", value=as_of - timedelta(days=27))
            po_end = st.date_input("PO end date", value=as_of)
            if po_end < po_start:
                st.error("PO end date must be on or after PO start date.")
                st.stop()
            if (po_end - po_start).days > 120:
                st.warning("PO range above 120 days can reduce decision quality.")
        if horizon == "Season":
            if not all_seasons:
                st.warning("No seasons found; switch to PO or 4 weeks.")
                st.stop()
            non_unknown_seasons = [s for s in all_seasons if str(s).strip() and s != "Unknown"]
            season_options = non_unknown_seasons or all_seasons
            season = st.selectbox("Season", season_options, index=0)
        min_confidence = st.selectbox("Confidence floor", ["Low", "Medium", "High"])
        rank_mode = st.selectbox("Action Queue ranking", RANK_MODES, index=0)
    scoped = normalized[
        normalized["brand"].isin(selected_brands)
        & normalized["size_group"].isin(selected_size_groups)
    ].copy()
    metrics = scoped.copy()
    if horizon == "4 weeks":
        end = pd.Timestamp(as_of)
        start = end - pd.Timedelta(days=27)
        metrics = metrics[(metrics["order_date"] >= start) & (metrics["order_date"] <= end)]
        horizon_days = 28
    elif horizon == "PO":
        start = pd.Timestamp(po_start)
        end = pd.Timestamp(po_end)
        metrics = metrics[(metrics["order_date"] >= start) & (metrics["order_date"] <= end)]
        horizon_days = max((end - start).days + 1, 1)
    else:
        metrics = metrics[metrics["season"] == str(season)]
        horizon_days = 90
    return metrics, {
        "horizon": horizon,
        "horizon_days": horizon_days,
        "as_of": as_of,
        "min_confidence": min_confidence,
        "rank_mode": rank_mode,
    }


def _confidence_label(sold_units: float, coverage: float, freshness_days: int, fallback_used: bool) -> str:
    if sold_units >= 300 and coverage >= 0.8 and freshness_days <= 14 and not fallback_used:
        return "High"
    if sold_units < 100 or coverage < 0.5 or fallback_used:
        return "Low"
    return "Medium"


def _compute_kpis(df: pd.DataFrame, horizon_days: int) -> tuple[float, float, float]:
    daily_rate = (df["sales_qty"].sum() / max(horizon_days, 1)) if not df.empty else 0
    expected_units = max(daily_rate * horizon_days, 0)
    lost_revenue = float(((expected_units - df["stock_qty"]).clip(lower=0) * df["list_price"]).sum())
    overstock_units = (df["stock_qty"] - df["sales_qty"]).clip(lower=0)
    margin_eroded = float((overstock_units * df["list_price"] * 0.2).sum())
    sell_through = df["sales_qty"] / df["stock_qty"].replace(0, pd.NA)
    capital_at_risk = float((df["stock_qty"] * df["unit_cost"] * (sell_through.fillna(0) < 0.3).astype(float)).sum())
    return lost_revenue, margin_eroded, capital_at_risk


def render_executive_dashboard(df: pd.DataFrame, horizon_days: int) -> None:
    st.subheader("The Bleed")
    lost_revenue, margin_eroded, working_capital = _compute_kpis(df, horizon_days)
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Lost Revenue", f"EUR {lost_revenue:,.0f}")
    c2.metric("Total Margin Eroded", f"EUR {margin_eroded:,.0f}")
    c3.metric("Working Capital at Risk", f"EUR {working_capital:,.0f}")
    by_size = df.groupby("size", dropna=False)[["buy_qty", "sales_qty"]].sum().reset_index()
    by_size["historical_buy_pct"] = by_size["buy_qty"] / max(by_size["buy_qty"].sum(), 1)
    by_size["true_demand_pct"] = by_size["sales_qty"] / max(by_size["sales_qty"].sum(), 1)
    source = by_size.melt(id_vars="size", value_vars=["historical_buy_pct", "true_demand_pct"], var_name="metric", value_name="pct")
    metric_labels = ["historical_buy_pct", "true_demand_pct"]
    chart = alt.Chart(source).mark_bar().encode(
        x=alt.X("size:N", title="Size"),
        y=alt.Y("pct:Q", axis=alt.Axis(format="%"), title="Share"),
        xOffset="metric:N",
        color=alt.Color(
            "metric:N",
            title="",
            sort=metric_labels,
            scale=alt.Scale(
                domain=metric_labels,
                range=[SERIES_COLORS["historical_buy_pct"], SERIES_COLORS["true_demand_pct"]],
            ),
            legend=alt.Legend(
                orient="bottom",
                labelExpr="datum.label == 'historical_buy_pct' ? 'Historical Buy %' : 'True Demand %'",
            ),
        ),
    ).properties(height=380, title="Historical Buy % vs True Demand %")
    st.altair_chart(chart, use_container_width=True)


def render_mismatch_drilldown(df: pd.DataFrame) -> None:
    st.subheader("The Mismatch")
    if df.empty:
        st.warning("No rows available for current global filters.")
        return

    c1, c2 = st.columns(2)
    categories = sorted(df["category"].dropna().unique().tolist())
    category = c1.selectbox("Category", ["All"] + categories)

    scoped = df.copy()
    if category != "All":
        scoped = scoped[scoped["category"] == category]

    fit_options = sorted(scoped["fit"].dropna().unique().tolist())
    fit = c2.selectbox("Fit", ["All"] + fit_options)
    if fit != "All":
        scoped = scoped[scoped["fit"] == fit]
    mismatch = scoped.groupby("size", dropna=False)[["buy_qty", "sales_qty"]].sum().reset_index()
    mismatch["buy_share"] = mismatch["buy_qty"] / max(mismatch["buy_qty"].sum(), 1)
    mismatch["demand_share"] = mismatch["sales_qty"] / max(mismatch["sales_qty"].sum(), 1)
    mismatch["gap_pp"] = (mismatch["demand_share"] - mismatch["buy_share"]) * 100
    source = mismatch.melt(id_vars="size", value_vars=["buy_share", "demand_share"], var_name="series", value_name="pct")
    mismatch_labels = ["buy_share", "demand_share"]
    chart = alt.Chart(source).mark_bar().encode(
        x=alt.X("size:N", title="Size"),
        y=alt.Y("pct:Q", axis=alt.Axis(format="%"), title="Share"),
        xOffset="series:N",
        color=alt.Color(
            "series:N",
            title="",
            sort=mismatch_labels,
            scale=alt.Scale(
                domain=mismatch_labels,
                range=[SERIES_COLORS["buy_share"], SERIES_COLORS["demand_share"]],
            ),
            legend=alt.Legend(
                orient="bottom",
                labelExpr="datum.label == 'buy_share' ? 'Buy Share' : 'Demand Share'",
            ),
        ),
    ).properties(height=380, title="Supply vs Demand by Size")
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(mismatch.rename(columns={"buy_qty": "buy_units", "sales_qty": "demand_units", "gap_pp": "gap_percentage_points"}), use_container_width=True)


def _profile_recommendation(df: pd.DataFrame, brand: str, category: str, fit: str, size_group: str, target_qty: int, style: str) -> tuple[pd.DataFrame, dict]:
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
    size_curve = local.groupby("size", dropna=False)["sales_qty"].sum().reset_index()
    if size_curve.empty:
        return pd.DataFrame(), {"fallback_used": fallback_used, "sold_units": sold_units, "confidence": "Low"}
    total_sales = max(size_curve["sales_qty"].sum(), 1)
    size_curve["recommended_share"] = size_curve["sales_qty"] / total_sales
    alloc = _largest_remainder_alloc(size_curve["recommended_share"], target_qty, size_curve["sales_qty"])
    size_curve["recommended_qty"] = alloc.values
    coverage = len(local[REQUIRED_KEYS + ["sales_qty"]].dropna()) / max(len(local), 1)
    latest = pd.to_datetime(local["order_date"], errors="coerce").max()
    freshness = (pd.Timestamp.today() - latest).days if pd.notna(latest) else 999
    confidence = _confidence_label(sold_units, coverage, freshness, fallback_used)
    meta = {"fallback_used": fallback_used, "sold_units": sold_units, "coverage": coverage, "freshness_days": freshness, "confidence": confidence}
    return size_curve.sort_values("size"), meta


def render_optimization_engine(df: pd.DataFrame) -> None:
    st.subheader("Optimization Engine")
    brands = sorted(df["brand"].dropna().unique().tolist())
    if not brands:
        st.warning("No brands available for the current global filters.")
        return

    c1, c2, c3, c4 = st.columns(4)
    brand = c1.selectbox("Sub-brand", brands)

    brand_df = df[df["brand"] == brand].copy()
    if brand_df.empty:
        st.warning("No rows found for the selected brand in current scope.")
        return

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
    rec, meta = _profile_recommendation(df, brand, category, fit, size_group, int(target_qty), style)
    if rec.empty:
        st.warning("No rows found for this profile.")
        return
    if meta["fallback_used"]:
        st.warning("Low sample: using broader baseline.")
    st.caption(
        f"Confidence: {meta['confidence']} | Sold units: {meta['sold_units']:.0f} | "
        f"Coverage: {meta.get('coverage', 0):.1%} | Freshness: {meta.get('freshness_days', 999)} days"
    )
    out = rec.rename(columns={"size": "Size", "recommended_share": "Share", "recommended_qty": "Buy Qty"})[["Size", "Share", "Buy Qty"]]
    st.dataframe(out, use_container_width=True)
    st.download_button("Download recommendation (CSV)", out.to_csv(index=False).encode("utf-8"), file_name=f"recommended_size_curve_{brand}_{category}_{fit}.csv".replace(" ", "_"), mime="text/csv")


def render_forecast_confidence(df: pd.DataFrame, cfg: dict) -> None:
    st.subheader("Forecast & Confidence")
    grouped = df.groupby("size", dropna=False)["sales_qty"].sum().reset_index()
    if grouped.empty:
        st.info("No demand rows available for this scope.")
        return
    horizon_days = int(cfg.get("horizon_days", 28))
    base = grouped["sales_qty"] / max(horizon_days, 1)
    grouped["base"] = base * horizon_days
    grouped["conservative"] = grouped["base"] * 0.85
    grouped["optimistic"] = grouped["base"] * 1.15
    if df["sales_qty"].sum() < 100:
        grouped["conservative"] = grouped["base"] * 0.75
        grouped["optimistic"] = grouped["base"] * 1.25
    source = grouped.melt(id_vars="size", value_vars=["conservative", "base", "optimistic"], var_name="scenario", value_name="units")
    scenario_labels = ["conservative", "base", "optimistic"]
    chart = alt.Chart(source).mark_line(point=True).encode(
        x=alt.X("size:N", title="Size"),
        y=alt.Y("units:Q", title="Projected units"),
        color=alt.Color(
            "scenario:N",
            title="Scenario",
            sort=scenario_labels,
            scale=alt.Scale(
                domain=scenario_labels,
                range=[SERIES_COLORS["conservative"], SERIES_COLORS["base"], SERIES_COLORS["optimistic"]],
            ),
            legend=alt.Legend(
                orient="bottom",
                labelExpr=(
                    "datum.label == 'conservative' ? 'Conservative' : "
                    "(datum.label == 'base' ? 'Base' : 'Optimistic')"
                ),
            ),
        ),
    ).properties(height=380)
    st.altair_chart(chart, use_container_width=True)
    freshness = (pd.Timestamp.today() - df["order_date"].max()).days if not df["order_date"].isna().all() else 999
    coverage = len(df[REQUIRED_KEYS + ["sales_qty"]].dropna()) / max(len(df), 1)
    conf = _confidence_label(float(df["sales_qty"].sum()), coverage, int(freshness), False)
    st.info(f"Confidence: {conf} | Sold units: {df['sales_qty'].sum():.0f} | Coverage: {coverage:.1%} | Freshness: {freshness} days")


def render_action_queue(df: pd.DataFrame, rank_mode: str, confidence_floor: str) -> None:
    st.subheader("Action Queue")
    profile = (
        df.groupby(["brand", "category", "fit", "size_group"], dropna=False)
        .agg(sold_units=("sales_qty", "sum"), buy_units=("buy_qty", "sum"), stock_units=("stock_qty", "sum"), avg_price=("list_price", "mean"))
        .reset_index()
    )
    profile["missed_revenue_raw"] = (profile["sold_units"] - profile["stock_units"]).clip(lower=0) * profile["avg_price"]
    profile["markdown_risk_raw"] = (profile["stock_units"] - profile["sold_units"]).clip(lower=0) * profile["avg_price"] * 0.2
    profile["mismatch_raw"] = (profile["buy_units"] - profile["sold_units"]).abs()
    profile["missed_revenue_score"] = _normalize_0_100(profile["missed_revenue_raw"])
    profile["markdown_risk_score"] = _normalize_0_100(profile["markdown_risk_raw"])
    profile["mismatch_severity_score"] = _normalize_0_100(profile["mismatch_raw"])
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
    st.download_button("Export action shortlist", output.to_csv(index=False).encode("utf-8"), file_name="action_queue.csv", mime="text/csv")


st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ["Executive Summary", "Demand Analysis", "Optimization Engine", "Forecast & Confidence", "Action Queue"],
)

with st.sidebar.expander("Database Check", expanded=False):
    cfg = get_postgres_config()
    st.caption(f"Host: `{cfg['host']}` | Port: `{cfg['port']}`\n\nDB: `{cfg['database']}` | Schema: `{cfg['schema']}`")
    if st.button("Test PostgreSQL Connection", use_container_width=True):
        try:
            engine = create_engine(build_connection_url(cfg))
            tables = list_schema_tables(engine, cfg["schema"])
            st.success("Connected successfully.")
            st.dataframe(pd.DataFrame({"table_name": tables}), use_container_width=True)
        except Exception as exc:
            st.error("Connection failed.")
            st.code(str(exc))

st.title(page)

try:
    data = load_and_merge_data()
    scoped_data, control_cfg = apply_global_controls(data)
    if scoped_data.empty:
        st.warning("No rows match current filters/horizon.")
        st.stop()
    if page == "Executive Summary":
        render_executive_dashboard(scoped_data, int(control_cfg.get("horizon_days", 28)))
    elif page == "Demand Analysis":
        render_mismatch_drilldown(scoped_data)
    elif page == "Optimization Engine":
        render_optimization_engine(scoped_data)
    elif page == "Forecast & Confidence":
        render_forecast_confidence(scoped_data, control_cfg)
    else:
        render_action_queue(scoped_data, control_cfg["rank_mode"], control_cfg["min_confidence"])
except Exception as exc:
    st.warning("Unable to load PostgreSQL data. Check `.streamlit/secrets.toml` and dependencies.")
    st.code(str(exc))
