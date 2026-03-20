"""Altair chart builders — explicit encodings, rubric-aligned palette."""
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from data_loader import SERIES_COLORS

# Neutral / historical baseline (slate grays + muted blue)
VIZ_NEUTRAL = "#94a3b8"
VIZ_NEUTRAL_DEEP = "#64748b"
VIZ_NEUTRAL_LIGHT = "#cbd5e1"
# Single accent: demand, gap, optimal action
VIZ_ACCENT = "#ea580c"


def render_bleed_chart(df: pd.DataFrame, horizon_days: int) -> None:
    _ = horizon_days
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
        .mark_bar(cornerRadiusEnd=2)
        .encode(
            x=alt.X("size:N", title="Size", axis=alt.Axis(labelAngle=-0, tickMinStep=1)),
            y=alt.Y("pct:Q", axis=alt.Axis(format="%", title="Share of category"), scale=alt.Scale(domainMin=0)),
            xOffset=alt.XOffset("metric:N", sort=metric_labels),
            color=alt.Color(
                "metric:N",
                title=None,
                sort=metric_labels,
                scale=alt.Scale(
                    domain=metric_labels,
                    range=[SERIES_COLORS["historical_buy_pct"], SERIES_COLORS["true_demand_pct"]],
                ),
                legend=alt.Legend(
                    orient="bottom",
                    labelExpr=(
                        "datum.label == 'historical_buy_pct' ? 'Historical buy mix' : 'True demand (highlight)'"
                    ),
                ),
            ),
            tooltip=[
                alt.Tooltip("size:N", title="Size"),
                alt.Tooltip("metric:N", title="Series"),
                alt.Tooltip("pct:Q", format=".1%", title="Share"),
            ],
        )
        .properties(height=380, title="Historical buy % vs true demand %")
        .configure_axis(grid=True, gridColor="#f1f5f9")
        .configure_view(strokeWidth=0)
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
        .mark_bar(cornerRadiusEnd=2)
        .encode(
            x=alt.X("size:N", title="Size"),
            y=alt.Y("pct:Q", axis=alt.Axis(format="%", title="Share"), scale=alt.Scale(domainMin=0)),
            xOffset=alt.XOffset("series:N", sort=mismatch_labels),
            color=alt.Color(
                "series:N",
                title=None,
                sort=mismatch_labels,
                scale=alt.Scale(
                    domain=mismatch_labels,
                    range=[SERIES_COLORS["buy_share"], SERIES_COLORS["demand_share"]],
                ),
                legend=alt.Legend(
                    orient="bottom",
                    labelExpr="datum.label == 'buy_share' ? 'Buy (neutral)' : 'Demand (highlight)'",
                ),
            ),
            tooltip=[
                alt.Tooltip("size:N", title="Size"),
                alt.Tooltip("pct:Q", format=".1%", title="Share"),
            ],
        )
        .properties(height=380, title="Buy share vs demand share by size")
        .configure_axis(grid=True, gridColor="#f1f5f9")
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(chart, use_container_width=True)


def render_forecast_chart(df: pd.DataFrame, horizon_days: int) -> None:
    """Scenario fan: muted grays + blue for base projection (no loud multi-hue clutter)."""
    grouped = df.groupby("size", dropna=False)["sales_qty"].sum().reset_index()
    base_rate = grouped["sales_qty"] / max(horizon_days, 1)
    grouped["base"] = base_rate * horizon_days
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
        .mark_line(point=alt.OverlayMarkDef(filled=True, size=50))
        .encode(
            x=alt.X("size:N", title="Size"),
            y=alt.Y("units:Q", title="Projected units (horizon)", scale=alt.Scale(domainMin=0)),
            color=alt.Color(
                "scenario:N",
                title=None,
                sort=scenario_labels,
                # Muted bands + orange accent on the base / demand anchor scenario.
                scale=alt.Scale(
                    domain=scenario_labels,
                    range=[VIZ_NEUTRAL_LIGHT, VIZ_ACCENT, VIZ_NEUTRAL_LIGHT],
                ),
                legend=alt.Legend(
                    orient="bottom",
                    labelExpr=(
                        "datum.label == 'conservative' ? 'Conservative band' : "
                        "(datum.label == 'base' ? 'Base demand (highlight)' : 'Optimistic band')"
                    ),
                ),
            ),
            tooltip=[
                alt.Tooltip("size:N", title="Size"),
                alt.Tooltip("scenario:N", title="Scenario"),
                alt.Tooltip("units:Q", format=",.0f", title="Units"),
            ],
        )
        .properties(height=380, title="Demand projection by size")
        .configure_axis(grid=True, gridColor="#f1f5f9")
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(chart, use_container_width=True)


def render_optimization_po_curve_comparison(detail: pd.DataFrame) -> None:
    """Grouped bars: share of target PO under historical buy curve vs demand-optimal curve."""
    if detail.empty:
        return
    size_order = detail["Size"].astype(str).tolist()
    src = detail.melt(
        id_vars=["Size"],
        value_vars=["po_share_historical_curve", "po_share_optimal_curve"],
        var_name="curve",
        value_name="share_of_po",
    )
    curve_domain = ["po_share_historical_curve", "po_share_optimal_curve"]
    chart = (
        alt.Chart(src)
        .mark_bar(cornerRadiusEnd=2)
        .encode(
            x=alt.X("Size:N", title="Size", sort=size_order),
            y=alt.Y(
                "share_of_po:Q",
                axis=alt.Axis(format="%", title="Share of target buy quantity"),
                scale=alt.Scale(domainMin=0),
            ),
            xOffset=alt.XOffset("curve:N", sort=curve_domain),
            color=alt.Color(
                "curve:N",
                title=None,
                sort=curve_domain,
                scale=alt.Scale(
                    domain=curve_domain,
                    range=[VIZ_NEUTRAL, VIZ_ACCENT],
                ),
                legend=alt.Legend(
                    orient="bottom",
                    labelExpr=(
                        "datum.label == 'po_share_historical_curve' ? 'Historical buy curve (at target qty)' : "
                        "'Data-driven optimal curve'"
                    ),
                ),
            ),
            tooltip=[
                alt.Tooltip("Size:N", title="Size"),
                alt.Tooltip("share_of_po:Q", format=".1%", title="PO share"),
            ],
        )
        .properties(
            height=400,
            title="Historical buy curve vs data-driven optimal (same PO quantity, largest-remainder allocation)",
        )
        .configure_axis(grid=True, gridColor="#f1f5f9")
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(chart, use_container_width=True)


def render_optimization_gap_and_margin(detail: pd.DataFrame) -> None:
    """Left: PO share variance (pp). Right: projected € impact per size (accent = positive opportunity)."""
    if detail.empty:
        return
    size_order = detail["Size"].astype(str).tolist()

    gap = detail[["Size", "variance_pp"]].copy()
    bar_gap = (
        alt.Chart(gap)
        .mark_bar(cornerRadiusEnd=2)
        .encode(
            x=alt.X("Size:N", sort=size_order, title="Size"),
            y=alt.Y("variance_pp:Q", axis=alt.Axis(title="Δ PO share (optimal − historical), pp")),
            color=alt.condition(
                alt.datum.variance_pp > 0,
                alt.value(VIZ_ACCENT),
                alt.condition(alt.datum.variance_pp < 0, alt.value(VIZ_NEUTRAL_DEEP), alt.value(VIZ_NEUTRAL_LIGHT)),
            ),
            tooltip=[
                alt.Tooltip("Size:N", title="Size"),
                alt.Tooltip("variance_pp:Q", format="+.1f", title="Δ pp"),
            ],
        )
        .properties(height=320, title="Actionable gap — where the optimal mix shifts allocation")
    )

    margin = detail[["Size", "projected_margin_eur"]].copy()
    bar_margin = (
        alt.Chart(margin)
        .mark_bar(cornerRadiusEnd=2)
        .encode(
            x=alt.X("Size:N", sort=size_order, title="Size"),
            y=alt.Y(
                "projected_margin_eur:Q",
                axis=alt.Axis(title="Projected € impact (heuristic)"),
                scale=alt.Scale(domainMin=0),
            ),
            color=alt.condition(
                alt.datum.projected_margin_eur > 0,
                alt.value(VIZ_ACCENT),
                alt.value(VIZ_NEUTRAL_DEEP),
            ),
            tooltip=[
                alt.Tooltip("Size:N", title="Size"),
                alt.Tooltip("projected_margin_eur:Q", format=",.0f", title="€"),
            ],
        )
        .properties(height=320, title="Per-size € lift from moving toward the optimal curve")
    )

    combined = (
        (bar_gap | bar_margin)
        .resolve_scale(color="independent")
        .configure_axis(grid=True, gridColor="#f1f5f9")
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(combined, use_container_width=True)
