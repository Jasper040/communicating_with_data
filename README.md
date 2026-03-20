# Communicating With Data — Buying Control Tower

Streamlit multipage app with Postgres-backed analytics and an **LLM SQL agent** (LangChain).

## Setup

```bash
python -m venv .venv
.\.venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

### Secrets (`.streamlit/secrets.toml`)

```toml
[postgres]
host = "localhost"
port = "5432"
database = "postgres"
user = "postgres"
password = "..."
schema = "public"

[openai]
api_key = "sk-..."
model = "gpt-4o-mini"
```

## Run

From the project root (so Streamlit sees the `pages/` folder next to `streamlit_app.py`):

```bash
streamlit run streamlit_app.py
```

**Multipage note:** `st.set_page_config` must run *before* importing `app_shell` / `data_loader` (those modules use `@st.cache_data`, which registers Streamlit hooks at import time). All app scripts follow that order so sidebar pages open correctly.

If pages still don’t appear, confirm you’re not running a file inside `pages/` directly; always use `streamlit run streamlit_app.py`.

## Data merge (SQL, not `SELECT *`)

Merged facts are loaded in **one query** with joins and aggregation in PostgreSQL (`data_loader.load_and_merge_data`).  
If your physical column names differ (e.g. `qty` instead of `quantity`), set env vars such as `PG_SALES_QTY_COL`, `PG_INV_STOCK_COL`, etc., or adjust `data_loader.py`.

Optional DBA view DDL: `docs/sql/buying_facts_merge_view.sql`.

## Layout

| Path | Purpose |
|------|---------|
| `streamlit_app.py` | Home + global sidebar |
| `pages/*.py` | Executive, demand, optimization, forecast, action queue, Ask the Data |
| `data_loader.py` | SQL merge, KPIs, recommendations |
| `app_shell.py` | Shared sidebar filters + DB check |
| `sql_agent.py` | LangChain SQL agent |
| `charts.py` | Altair charts |
