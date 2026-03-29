# Communicating With Data — Buying Control Tower

Streamlit multipage app with PostgreSQL-backed analytics and an **LLM SQL agent** (LangChain + OpenAI) on the **Ask the Data** page.

---

## Prerequisites

- **Python** 3.11 or newer (3.12+ recommended for LangChain/Streamlit compatibility).
- **PostgreSQL** with a database you can connect to over TCP.
- **OpenAI API key** if you use **Ask the Data** (`pages/6_Ask_the_Data.py`). Other pages only need Postgres.

---

## 1. Clone and Python environment

```bash
git clone <repository-url> communicating_with_data
cd communicating_with_data

python -m venv .venv
```

Activate the virtual environment:

- **Windows (PowerShell):** `.\.venv\Scripts\Activate.ps1`
- **Windows (cmd):** `.\.venv\Scripts\activate.bat`
- **macOS / Linux:** `source .venv/bin/activate`

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## 2. PostgreSQL schema and tables

The app expects **four tables in one schema**, with these **exact table names**:

| Table | Role |
|--------|------|
| `total_sales_b2c` | B2C sales lines (quantity, dates, brand, season, SKU keys) |
| `inventory` | On-hand stock by SKU |
| `purchased` | Purchase order lines |
| `products` | Product master (list price, brand, season, attributes) |

**Merge grain:** each analytics row is keyed by `(item_no, colour_no, size, barcode)` — those columns must exist on all four tables (nullable keys may produce sparse joins).

### Create empty tables (reference DDL)

From the repo root, apply the bundled DDL against your target database (defaults to `public`):

```bash
psql -h localhost -U postgres -d postgres -f update_schema.sql
```

Adjust host, user, and database name to match your instance. The file is `update_schema.sql` at the project root; it uses `CREATE TABLE IF NOT EXISTS` with column names aligned to the app’s defaults.

### Load data

Insert or ETL your own rows into those tables. The app does not ship sample data. Empty tables will load but filters may show no scope until data exists.

### Optional: DBA view

`docs/sql/buying_facts_merge_view.sql` is an optional PostgreSQL view that mirrors the merge logic in code. Replace `your_schema` in that file before running it.

---

## 3. Streamlit secrets (required for normal local use)

Create **`.streamlit/secrets.toml`** next to `streamlit_app.py` (the `.streamlit` folder is already in the repo for `config.toml`). **Do not commit real passwords or API keys.**

Minimal template:

```toml
[postgres]
host = "localhost"
port = "5432"
database = "postgres"
user = "postgres"
password = "your_password"
schema = "public"

[openai]
api_key = "sk-..."
model = "gpt-4o-mini"
```

- **`schema`:** PostgreSQL schema that contains the four tables (often `public`).
- **`[openai]`:** Required only for **Ask the Data**. You can omit `[openai]` if you never open that page; if you open it without a key, the app will error until you set a key (see environment variables below).

Values in `secrets.toml` take precedence over environment variables when both are set (see `data_loader.get_postgres_config` and `sql_agent._openai_llm`).

---

## 4. Environment variables (alternative to secrets)

If you prefer not to use `secrets.toml` for connection info (for example in automation), set:

| Variable | Purpose |
|----------|---------|
| `PGHOST` | Postgres host |
| `PGPORT` | Port (default `5432`) |
| `PGDATABASE` | Database name |
| `PGUSER` | User |
| `PGPASSWORD` | Password |
| `PGSCHEMA` | Schema (default `public`) |

For the SQL agent / OpenAI:

| Variable | Purpose |
|----------|---------|
| `OPENAI_API_KEY` | API key if not in `secrets.toml` |
| `OPENAI_MODEL` | Model name (default `gpt-4o-mini` if not in secrets) |

### Column mapping and business rules (optional)

If your physical column names differ from the defaults in `data_loader.py`, set:

- **Sales (`total_sales_b2c`):** `PG_SALES_QTY_COL`, `PG_SALES_DATE_COL`, `PG_SALES_SEASON_COL`, `PG_SALES_BRAND_COL`, `PG_SALES_REVENUE_COL` (optional explicit revenue column), or **`PG_SALES_EX_VAT_AMOUNT_COL`** (required if autodetection cannot find a suitable amount column).
- **Inventory:** `PG_INV_STOCK_COL`
- **Purchased:** `PG_PUR_QTY_COL`, `PG_PUR_PRICE_COL`, `PG_PUR_SEASON_COL`
- **Products:** `PG_PROD_LISTPRICE_COL`, `PG_PROD_SEASON_COL`, `PG_PROD_BRAND_COL`, `PG_PROD_ITEM_GROUP_COL`, `PG_PROD_FIT_COL`, `PG_PROD_SIZE_GROUP_COL`

**Sales amount column:** the loader sums one ex-VAT (or consistent) turnover column per SKU. If inference fails, set `PG_SALES_EX_VAT_AMOUNT_COL` or `PG_SALES_REVENUE_COL` to a real column name on `total_sales_b2c`.

**NL VAT / list price:** product list price is treated as **including 21% NL VAT**; sales amounts are **ex VAT**. Override divisor with `NL_VAT_DIVISOR` (default `1.21`).

**Other tunables:** `MARGIN_WRITE_OFF_FLOOR`, `FREE_OR_GIVEAWAY_UNIT_REVENUE_MAX_EUR`, `STOCKOUT_WRITE_OFF_WEEKS_AFTER_FIRST_SALE` — see comments in `data_loader.py`.

---

## 5. Run the app

Always start from the **repository root** so Streamlit sees `pages/` beside `streamlit_app.py`:

```bash
streamlit run streamlit_app.py
```

Then open the URL Streamlit prints (usually `http://localhost:8501`).

### Multipage / `set_page_config`

`st.set_page_config` runs **first** in each script; modules such as `app_shell` and `data_loader` are imported **after** it because they use `@st.cache_data`, which registers hooks at import time. Do not run a file inside `pages/` directly as the entrypoint — always use `streamlit run streamlit_app.py`.

### Smoke test

In the sidebar, open **Database Check** and use **Test PostgreSQL Connection**. If the merged load fails, the sidebar shows the exception — often missing tables, wrong `schema`, or an unset / wrong sales amount column.

---

## 6. Project layout

| Path | Purpose |
|------|---------|
| `streamlit_app.py` | Home page + global `set_page_config` |
| `pages/1_Executive_Summary.py` | Executive view |
| `pages/2_Demand_Analysis.py` | Demand analysis |
| `pages/3_Optimization_Engine.py` | Optimization |
| `pages/4_Forecast_Confidence.py` | Forecast confidence |
| `pages/5_Action_Queue.py` | Action queue |
| `pages/6_Ask_the_Data.py` | Natural language SQL (LangChain + OpenAI) |
| `data_loader.py` | SQL merge, KPIs, recommendations |
| `app_shell.py` | Shared sidebar filters + DB check |
| `sql_agent.py` | LangChain SQL agent (restricted to the four tables) |
| `charts.py` | Altair charts |
| `update_schema.sql` | Reference `CREATE TABLE` for the four core tables |
| `docs/sql/buying_facts_merge_view.sql` | Optional merged view DDL |
| `.streamlit/config.toml` | Theme (primary accent, backgrounds) |

---

## 7. How data is merged (short)

Merged facts are loaded in **one SQL query** with joins and aggregation in PostgreSQL (`data_loader.load_and_merge_data`), not with a Python `SELECT *` merge. If you change column names, use the `PG_*` environment variables above or adjust `data_loader.py` consistently with your database.
