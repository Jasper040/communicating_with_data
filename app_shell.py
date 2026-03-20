"""
Shared Streamlit shell: sidebar filters (session-persisted), DB health check.
"""
from __future__ import annotations

import re
from datetime import timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

from data_loader import (
    RANK_MODES,
    build_connection_url,
    get_postgres_config,
    list_schema_tables,
    load_and_merge_data,
    with_common_metrics,
)


def _validate_schema_name(schema: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", str(schema)):
        raise ValueError("Invalid PostgreSQL schema name in configuration.")
    return str(schema)


def render_app_shell(*, require_non_empty_scope: bool = True) -> tuple[pd.DataFrame | None, dict | None]:
    """
    Load merged facts, render shared sidebar controls, return scoped dataframe + control config.
    """
    st.sidebar.title("Buying Control Tower")
    render_database_check()
    try:
        data = load_and_merge_data()
    except Exception as exc:
        st.sidebar.error("Data load failed — check secrets and SQL merge.")
        st.sidebar.code(str(exc))
        return None, None

    scoped_data, control_cfg = apply_global_controls(data)
    if scoped_data.empty:
        st.sidebar.warning("No rows match current filters/horizon.")
        if require_non_empty_scope:
            return None, None
    return scoped_data, control_cfg


def render_database_check() -> None:
    with st.sidebar.expander("Database Check", expanded=False):
        cfg = get_postgres_config()
        _validate_schema_name(cfg["schema"])
        st.caption(
            f"Host: `{cfg['host']}` | Port: `{cfg['port']}`\n\n"
            f"DB: `{cfg['database']}` | Schema: `{cfg['schema']}`"
        )
        if st.button("Test PostgreSQL Connection", use_container_width=True, key="db_test_btn"):
            try:
                engine = create_engine(build_connection_url(cfg))
                tables = list_schema_tables(engine, cfg["schema"])
                st.success("Connected successfully.")
                st.dataframe(pd.DataFrame({"table_name": tables}), use_container_width=True)
            except Exception as exc:
                st.error("Connection failed.")
                st.code(str(exc))


def apply_global_controls(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Render global filters in the sidebar. Widget keys keep selections stable across multipage navigation.
    """
    normalized = with_common_metrics(df)
    all_brands = sorted(normalized["brand"].dropna().unique().tolist())
    all_size_groups = sorted(normalized["size_group"].dropna().unique().tolist())
    all_seasons = sorted(normalized["season"].dropna().unique().tolist(), reverse=True)
    order_dates = pd.to_datetime(normalized["order_date"], errors="coerce")
    default_as_of_ts = order_dates.max() if order_dates.notna().any() else pd.Timestamp.today()
    default_as_of = default_as_of_ts.date()

    with st.sidebar.expander("Global Filters", expanded=True):
        selected_brands = st.multiselect(
            "Brands",
            all_brands,
            default=all_brands,
            key="gc_brands",
        )
        selected_size_groups = st.multiselect(
            "Size Groups",
            all_size_groups,
            default=all_size_groups,
            key="gc_size_groups",
        )
        horizon = st.selectbox("Horizon", ["Season", "4 weeks", "PO"], key="gc_horizon")
        as_of = st.date_input("As-of date", value=default_as_of, key="gc_as_of")
        po_start, po_end, season = None, None, None
        if horizon == "PO":
            po_start = st.date_input("PO start date", value=as_of - timedelta(days=27), key="gc_po_start")
            po_end = st.date_input("PO end date", value=as_of, key="gc_po_end")
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
            season = st.selectbox("Season", season_options, index=0, key="gc_season")
        min_confidence = st.selectbox("Confidence floor", ["Low", "Medium", "High"], key="gc_conf_floor")
        rank_mode = st.selectbox("Action Queue ranking", RANK_MODES, index=0, key="gc_rank_mode")

    scoped = normalized[
        normalized["brand"].isin(selected_brands) & normalized["size_group"].isin(selected_size_groups)
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
