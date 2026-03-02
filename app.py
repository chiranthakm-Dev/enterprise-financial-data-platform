
import streamlit as st
import pandas as pd
from pathlib import Path
from typing import Optional


def load_csv(name: str) -> Optional[pd.DataFrame]:
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
