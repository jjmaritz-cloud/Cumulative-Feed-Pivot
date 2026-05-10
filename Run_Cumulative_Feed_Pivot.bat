@echo off
title Cumulative Feed Pivot

cd /d "C:\Cummilative Feed"

echo Starting Cumulative Feed Pivot...
echo.

REM Use Python launcher if available, otherwise fall back to python
where py >nul 2>nul
if %errorlevel%==0 (
    py -m streamlit run streamlit_feed_pivot_page.py
) else (
    python -m streamlit run streamlit_feed_pivot_page.py
)

pause
