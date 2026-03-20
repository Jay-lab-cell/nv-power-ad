@echo off
chcp 949 >nul
echo ========================================
echo  네이버 광고 ROAS 분석 실행
echo ========================================
echo.
python -m pip install streamlit pandas openpyxl altair --quiet
echo 브라우저에서 http://localhost:8501 로 접속하세요
python -m streamlit run "%~dp0app.py" --server.headless true
pause
