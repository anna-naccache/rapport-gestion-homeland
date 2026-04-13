@echo off
chcp 65001 > nul
title Debug HBO CSAT
echo.
echo =============================================
echo   Exploration API HBO - Routes CSAT
echo =============================================
echo.
cd /d "%~dp0"
python debug_hbo_csat.py
echo.
echo =============================================
echo   Script termine (voir erreurs ci-dessus)
echo =============================================
pause
