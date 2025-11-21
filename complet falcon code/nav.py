import streamlit as st

def nav_menu():
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("📊 NSE"):
            st.switch_page("pages/Nse_Dashboard.py")

    with col2:
        if st.button("📘 BSE"):
            st.switch_page("pages/Bse_dashboard.py")

    with col3:
        if st.button("📗 SME"):
            st.switch_page("pages/SME_dashboard.py")

    with col4:
        if st.button("📈 Chart"):
            st.switch_page("pages/Stock_Chart.py")

    st.markdown("---")
