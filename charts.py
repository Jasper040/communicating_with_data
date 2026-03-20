"""Altair chart builders shared across pages."""
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from data_loader import SERIES_COLORS


def render_bleed_chart(df: pd.DataFrame, horizon_days: int) -> None:
    _ = horizon_days  # reserved for future annotations
    by_size = df.groupby("size", dropna=False)[["buy_qty", "sales_qty"]].sum().reset_index()
    by_size["historical_buy_pct"] = by_size["buy_qty"] / max(by_size["buy_qty"].sum(), 1)
    by_size["true_demand_pct"] = by_size["sales_qty"] / max(by_size["sales_qty"].sum(), 1)
    source = by_size.melt(
        id_vars="size",
        value_vars=["historical_buy_pct", "true_demand_pct"],
        var_name="metric",
        value_name="pct",
    )
    metric_labels = ["historical_buy_pct", "true_demand_pct"]
    chart = (
        alt.Chart(source)
        .mark_bar()
        .encode(
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
        )
        .properties(height=380, title="Historical Buy % vs True Demand %")
    )
    st.altair_chart(chart, use_container_width=True)


def render_mismatch_chart(mismatch: pd.DataFrame) -> None:
    source = mismatch.melt(
        id_vars="size",
        value_vars=["buy_share", "demand_share"],
        var_name="series",
        value_name="pct",
    )
    mismatch_labels = ["buy_share", "demand_share"]
    chart = (
        alt.Chart(source)
        .mark_bar()
        .encode(
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
        )
        .properties(height=380, title="Supply vs Demand by Size")
    )
    st.altair_chart(chart, use_container_width=True)


def render_forecast_chart(df: pd.DataFrame, horizon_days: int) -> None:
    grouped = df.groupby("size", dropna=False)["sales_qty"].sum().reset_index()
    base = grouped["sales_qty"] / max(horizon_days, 1)
    grouped["base"] = base * horizon_days
    grouped["conservative"] = grouped["base"] * 0.85
    grouped["optimistic"] = grouped["base"] * 1.15
    if df["sales_qty"].sum() < 100:
        grouped["conservative"] = grouped["base"] * 0.75
        grouped["optimistic"] = grouped["base"] * 1.25
    source = grouped.melt(
        id_vars="size",
        value_vars=["conservative", "base", "optimistic"],
        var_name="scenario",
        value_name="units",
    )
    scenario_labels = ["conservative", "base", "optimistic"]
    chart = (
        alt.Chart(source)
        .mark_line(point=True)
        .encode(
            x=alt.X("size:N", title="Size"),
            y=alt.Y("units:Q", title="Projected units"),
            color=alt.Color(
                "scenario:N",
                title="Scenario",
                sort=scenario_labels,
                scale=alt.Scale(
                    domain=scenario_labels,
                    range=[
                        SERIES_COLORS["conservative"],
                        SERIES_COLORS["base"],
                        SERIES_COLORS["optimistic"],
                    ],
                ),
                legend=alt.Legend(
                    orient="bottom",
                    labelExpr=(
                        "datum.label == 'conservative' ? 'Conservative' : "
                        "(datum.label == 'base' ? 'Base' : 'Optimistic')"
                    ),
                ),
            ),
        )
        .properties(height=380)
    )
    st.altair_chart(chart, use_container_width=True)
