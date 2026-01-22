import streamlit as st

st.set_page_config(
    page_title="NSE Dashboard",
    layout="wide"
)

st.title("📌 NSE Analytics Dashboard")

st.write("Select a page from the sidebar to begin.")
import streamlit as st
from ui import load_global_ui

# Load UI theme ONCE
load_global_ui()

st.set_page_config(page_title="My Dashboard", layout="wide")

st.write("Welcome! Use sidebar to navigate pages.")
