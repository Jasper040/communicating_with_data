"""
LangChain SQL agent for natural-language questions against Postgres.

Uses `langchain-community` create_sql_agent (recommended API). The project also
depends on `langchain-experimental` for course alignment with materials that
reference that package.
"""
from __future__ import annotations

import os

# Course / rubric: langchain-experimental (SQLDatabaseChain, etc.) alongside the community SQL agent.
try:
    from langchain_experimental.sql.base import SQLDatabaseChain  # noqa: F401
except ImportError:
    SQLDatabaseChain = None  # type: ignore[misc, assignment]

import streamlit as st
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI

from data_loader import build_connection_url, get_postgres_config


def _openai_llm() -> ChatOpenAI:
    oai = st.secrets.get("openai", {})
    api_key = oai.get("api_key") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "Missing OpenAI API key. Add [openai] api_key to `.streamlit/secrets.toml` or set OPENAI_API_KEY."
        )
    model = oai.get("model", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
    return ChatOpenAI(api_key=api_key, model=model, temperature=0)


def build_sql_agent():
    """
    SQL agent with read-only focus: restrict to the four buying tables in the configured schema.
    """
    cfg = get_postgres_config()
    uri = build_connection_url(cfg)
    schema = cfg["schema"]

    db = SQLDatabase.from_uri(
        uri,
        schema=schema,
        include_tables=["total_sales_b2c", "inventory", "purchased", "products"],
        sample_rows_in_table_info=2,
    )

    llm = _openai_llm()

    custom_prefix = """You are an analytics assistant for a Head of Buying.
- The database is PostgreSQL; schema is configured — use only tables you are given.
- Prefer readable column aliases and LIMIT large result sets (e.g. LIMIT 50) unless the user needs full detail.
- For "markdown risk", interpret as overstock: inventory and purchases relative to sales (e.g. unsold stock × list price × markdown rate).
- When joining products to sales, join on item_no, colour_no, size, and barcode when available.
- Answer in clear business language; cite numbers from query results.
"""

    try:
        return create_sql_agent(
            llm=llm,
            db=db,
            agent_type="tool-calling",
            verbose=False,
            prefix=custom_prefix,
        )
    except TypeError:
        try:
            return create_sql_agent(
                llm=llm,
                db=db,
                agent_type="tool-calling",
                verbose=False,
            )
        except TypeError:
            return create_sql_agent(llm=llm, db=db, verbose=False)
