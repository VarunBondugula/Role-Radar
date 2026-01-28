import streamlit as st
import os, sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

    
st.set_page_config(page_title="RoleRadar", layout="wide")

st.title("RoleRadar")
st.caption("Phase 1: Greenhouse ingest → Postgres → Browse/Filter")
st.info("Use the Pages sidebar to open the Jobs browser.")