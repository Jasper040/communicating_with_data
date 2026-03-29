# Buying Control Tower Design

Date: 2026-03-20  
Project: Communicating With Data (Streamlit)  
Audience: Head of Buying

## 1) Goal

Design a ruthless, clean, and easy-to-use buying decision tool that prioritizes one core weekly outcome:

- Reallocate size curves for upcoming buys.

The tool must support:

- clear business impact visibility,
- size-group-aware diagnosis,
- actionable size buy recommendations,
- confidence-aware fallbacks,
- prioritized action ranking for fast execution.

## 2) Information Architecture (Approved Approach 1)

Navigation model: five distinct views with a global control rail.

1. The Bleed (Executive)
2. The Mismatch (Interactive Drill-Down)
3. Optimization Engine
4. Forecast & Confidence
5. Action Queue

Global controls are persistent across all pages:

- Brand (multi-select)
- Size Group (multi-select)
- Horizon selector (PO / 4 weeks / season)
- As-of date selector (single date, default = today in Europe/Amsterdam)
- PO date range selectors (start/end, shown only when Horizon = PO)
- Season selector (single-select, shown only when Horizon = season)
- Confidence floor toggle
- Ranking mode selector for action queue

Season selector contract:

- Allowed values are distinct season codes from `prod_season` in filtered scope.
- Default is most recent season code by lexical descending.
- If no season exists, season horizon is disabled with explanatory warning.

PO date range contract:

- `po_start_date` and `po_end_date` are required when Horizon = PO.
- Defaults:
  - `po_start_date` = as-of date
  - `po_end_date` = as-of date + 28 days
- Validation:
  - if end < start, block calculation and show inline error.
  - max range length = 120 days (guardrail warning if exceeded).

## 3) Core Data and Grain

Merge keys (strict):

- `item_no`, `colour_no`, `size`, `barcode`

Join and key policy:

- Primary join key set is the four-key tuple above.
- Join type is outer join to preserve demand-only and stock-only records.
- If `barcode` is missing in a source, the row is excluded from recommendation math and counted in data-quality diagnostics.
- Duplicate key rows are aggregated before merge using sum for quantities and latest non-null for attributes.

Recommendation grains:

- Primary grain: Brand + Category + Fit + Size Group
- Optional drill-down grain: Primary + Item family/style
- Fallback grain: Brand + Category + Fit (used only under sparse data)

Critical rule:

- The same size label can exist in multiple size groups.  
  All size comparisons and recommendations must be size-group scoped.

## 4) Decision Logic

### 4.0 Horizon Definitions (Canonical)

- PO horizon: user-provided PO window (`po_start_date` to `po_end_date`), inclusive.
- 4-week horizon: rolling 28 calendar days ending at selected as-of date.
- Season horizon: all rows where season equals selected season code.
- Timezone for date filters: Europe/Amsterdam.
- As-of date applies globally to all pages and calculations in session state.

### 4.6 Forecast Scenario Method

Forecast is deterministic and rule-based (no ML model in v1):

- Base forecast per size:
  - weighted average of recent demand rates:
    - 70% from latest 4-week rate
    - 30% from season-to-date rate
- Scale base rate by selected horizon length.
- Scenario bands:
  - Conservative = Base * 0.85
  - Optimistic = Base * 1.15
- If sample sold units < 100:
  - widen band to 0.75 / 1.25 and label low confidence.

### 4.1 Recommendation Generation

For a selected profile and horizon:

1. Aggregate sold units by size.
2. Compute demand share by size.
3. Convert demand share to recommended buy quantities based on target buy quantity.
4. Round quantities to integers and preserve exact total target quantity.

Rounding method:

- Use Largest Remainder:
  1) compute raw allocation = share * target quantity,
  2) floor all raw allocations,
  3) distribute remaining units to sizes with largest fractional remainders.
- Tie-break order: higher sold units first, then lexical size label.

### 4.2 Fallback Policy

If selected segment has fewer than 100 sold units:

- Trigger fallback to Brand + Category + Fit baseline.
- Show warning badge: "Low sample: using broader baseline".
- Keep output aligned to selected size group using this mapping rule:
  - If baseline has rows for selected size group, use only those.
  - Else map by exact size label intersection between selected group and baseline.
  - If intersection has fewer than 3 sizes, use historical buy distribution for selected size group.

If fallback remains sparse:

- Use conservative prior (historical buy distribution),
- Mark recommendation confidence as low.

### 4.3 Confidence Signals

Expose:

- sample sold units,
- data coverage ratio,
- fallback usage flag,
- data freshness indicator,
- confidence level.

Confidence bands:

- High: sold units >= 300 and coverage ratio >= 0.8 and data freshness <= 14 days.
- Medium: sold units 100-299 or coverage ratio 0.5-0.79.
- Low: sold units < 100 or coverage ratio < 0.5 or fallback invoked.

Coverage ratio definition:

- rows with complete key + required metric fields / total rows in selected scope.

### 4.4 KPI Formulas (Canonical)

- Total Lost Revenue:
  - `sum(max(0, expected_units - available_units) * list_price)` where `expected_units` is mean weekly demand scaled to horizon.
- Total Margin Eroded:
  - `sum(overstock_units * markdown_rate * list_price)` using default markdown_rate 30%.
- Working Capital at Risk:
  - `sum(on_hand_units * unit_cost)` for units with sell-through below threshold (default 30% in horizon).

### 4.5 Action Queue Scoring

Base scores (scaled 0-100):

- Missed Revenue Score = normalized lost revenue.
- Markdown Risk Score = normalized margin erosion risk.
- Mismatch Severity Score = normalized absolute buy-vs-demand share gap.

Normalization method (canonical):

- Use min-max scaling within current filtered scope and selected horizon:
  - `score = 100 * (x - min_x) / (max_x - min_x)`
- If `max_x == min_x`, set all rows to score 50.
- Clip to [0, 100] after scaling.
- Tie-break order for ranking:
  1) higher confidence level,
  2) higher expected upside,
  3) lexical profile id.

Blended score default weights:

- 40% Missed Revenue
- 35% Markdown Risk
- 25% Mismatch Severity

Ranking mode selects one score or blended score directly.

## 5) Page Design

## 5.1 The Bleed (Executive)

Purpose: Prove the problem and quantify damage.

Components:

- KPI cards:
  - Total Lost Revenue (stockout proxy)
  - Total Margin Eroded (markdown proxy)
  - Working Capital at Risk
- Primary chart:
  - Historical Buy % vs True Demand % by size (size-group-aware)
- Decision callout:
  - Top size distortions driving value loss in selected horizon

## 5.2 The Mismatch (Interactive Drill-Down)

Purpose: Explain structural supply-demand gaps.

Components:

- Local refiners:
  - Category
  - Fit
- Primary chart:
  - Supply vs demand by size (side-by-side bars or heatmap)
- Supporting table:
  - buy units, demand units, buy share, demand share, gap in percentage points

## 5.3 Optimization Engine

Purpose: Produce actionable buy recommendations.

Components:

- Inputs:
  - profile selector (summary level or style drill-down)
  - horizon
  - total target buy quantity (user-entered integer, required, min 1)
- Output:
  - recommended size distribution table
  - confidence metadata
  - fallback warning badge when active
- Action:
  - export recommendation to CSV

## 5.4 Forecast & Confidence

Purpose: Show expected volatility and recommendation reliability.

Components:

- Primary chart:
  - demand projection by size with scenario bands (base, conservative, optimistic)
- Confidence panel:
  - sample size, fallback usage, freshness, confidence grade

## 5.5 Action Queue

Purpose: Prioritize interventions for weekly buying cadence.

Components:

- Ranking mode toggle:
  - Missed Revenue
  - Markdown Risk
  - Mismatch Severity
  - Blended Score
- Priority table:
  - profile, issue type, expected upside/risk, confidence, suggested action
- Actions:
  - export shortlist
  - mark rows reviewed

## 6) UX Principles

- One primary chart per page.
- One primary table per page.
- Defaults should work without setup friction.
- Keep controls concise and decision-oriented.
- Use warnings only when they alter recommended action.
- Avoid decorative elements that do not improve decisions.

## 7) Error Handling

- Missing required merge keys: hard fail with explicit missing columns.
- Empty filter result: show clear "no rows match" guidance.
- DB connection issues: display actionable configuration error.
- Sparse segments: fallback behavior plus confidence badge, never silent degradation.

## 8) Validation and Testing

Functional checks:

- Global filters propagate consistently to all pages.
- Size-group scoping prevents cross-group size mixing.
- Fallback triggers exactly when sold units < 100.
- Target buy quantity is preserved after rounding.
- Action Queue rankings switch correctly by mode.
- Largest Remainder rounding preserves exact target quantity.
- Horizon filters return deterministic row sets for PO/4-week/season.

Business sanity checks:

- Recommendation shifts make directional sense vs demand.
- High-confidence segments produce stable outputs.
- Low-confidence segments are clearly flagged.

## 9) Phased Delivery

Phase 1:

- 5-view IA, global filter rail, strict merge/data foundations.

Phase 2:

- confidence metrics, fallback orchestration, action scoring calibration.

Phase 3:

- forecast refinement, export polishing, stakeholder UAT feedback loop.
