"""Simple Streamlit dashboard for viewing pipeline outputs.

This lightweight web app reads the CSV files produced by the ETL pipeline
(`data/processed/*`) and displays them in tables.  It is intended as a demo
so stakeholders can "see the output" without needing to query Snowflake.

Run with:

    pip install streamlit pandas
    streamlit run app.py

This will start a local web server on http://localhost:8501.
"""

import streamlit as st
import pandas as pd
from pathlib import Path


def load_csv(name: str) -> pd.DataFrame | None:
    path = Path("data/processed") / name
    if path.exists():
        return pd.read_csv(path)
    return None


st.title("Financial ETL Dashboard")

st.markdown("This demo loads the latest CSV outputs from `data/processed`."
            " Run the pipeline in dry-run mode to generate sample data.")

files = [f.name for f in Path("data/processed").glob("*.csv")]
if not files:
    st.warning("No processed CSV files found. Run the pipeline first.")
else:
    choice = st.selectbox("Choose file", files)
    df = load_csv(choice)
    if df is not None:
        st.write(df)
        if "amount" in df.columns:
            st.write("**Basic stats:**")
            st.write(df["amount"].describe())
    else:
        st.error(f"Failed to load {choice}")

st.sidebar.header("Quick Links")
st.sidebar.markdown("- [Run pipeline](#)\n- [View SQL files](sql/views)")
