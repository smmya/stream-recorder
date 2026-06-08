@echo off
chcp 65001 >nul
cd /d "%~dp0"
title 流媒体自动录制系统 - 后台运行
echo.
echo ============================================
echo   流媒体自动录制系统 v2.3
echo   后台模式启动中...
echo ============================================
echo.
python main.py --daemon
echo.
echo 按任意键关闭此窗口（服务仍在后台运行）
pause >nul
