#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
python -m pip install -r requirements.txt
python check_setup.py
python -m streamlit run streamlit_app.py
