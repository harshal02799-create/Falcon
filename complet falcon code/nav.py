
import streamlit as st

def nav_menu():
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col3:
        if st.button("📊 NSE"):
            st.switch_page("pages/Nse_Dashboard.py")

    with col5:
        if st.button("📘 BSE"):
            st.switch_page("pages/Bse_Dashboard.py")

    with col4:
        if st.button("📗 SME"):
            st.switch_page("pages/SME_dashboard.py")

    with col2:
        if st.button("📈 market breadth1.py"):
            st.switch_page("pages/market breadth1.py")

    with col6:
        if st.button("📈 Chart"):
            st.switch_page("pages/Stock_Chart.py")

    with col1:
        if st.button("📈 Sector Mover"):
            st.switch_page("pages/Sector Mover.py")

    st.markdown("---")
