import streamlit as st

st.set_page_config(page_title="Ask the Data | Buying", layout="wide")

from app_shell import render_app_shell
from sql_agent import build_sql_agent

# Shared sidebar (filters + DB) — merged load may fail while raw tables still work for the agent.
render_app_shell(require_non_empty_scope=False)

st.header("Ask the Data")
st.markdown(
    """
Ask questions in plain language. A **LangChain SQL agent** (OpenAI + Postgres) reads your four core tables:
`total_sales_b2c`, `inventory`, `purchased`, `products`.

Add **`[openai]`** with `api_key` (and optional `model`) to `.streamlit/secrets.toml`.
"""
)

if "ask_chat_messages" not in st.session_state:
    st.session_state.ask_chat_messages = []

for msg in st.session_state.ask_chat_messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

user_q = st.chat_input('Try: "Which categories have the highest markdown risk?"')
if user_q:
    st.session_state.ask_chat_messages.append({"role": "user", "content": user_q})
    with st.chat_message("user"):
        st.markdown(user_q)

    with st.chat_message("assistant"):
        try:
            agent = build_sql_agent()
            result = agent.invoke({"input": user_q})
            text_out = result.get("output") if isinstance(result, dict) else str(result)
            st.markdown(text_out)
            st.session_state.ask_chat_messages.append({"role": "assistant", "content": text_out})
        except Exception as exc:
            err = f"Could not run the SQL agent: `{exc}`"
            st.error(err)
            st.session_state.ask_chat_messages.append({"role": "assistant", "content": err})

if st.button("Clear conversation", key="ask_clear_chat"):
    st.session_state.ask_chat_messages = []
    st.rerun()
