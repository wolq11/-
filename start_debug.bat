@echo off
chcp 65001 >nul
echo.
echo  ============================================
echo   社团活动统计分析系统 - 调试模式启动
echo   访问地址: http://localhost:5000
echo  ============================================
echo.
echo  正在启动服务器（调试模式）...
echo.

"D:\Anaconda3\python.exe" "%~dp0server.py" --debug

if errorlevel 1 (
    echo.
    echo  [错误] 启动失败，请检查 Python 环境
    pause
)

