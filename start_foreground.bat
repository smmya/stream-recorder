@echo off
chcp 65001 >nul
cd /d "%~dp0"
title 流媒体自动录制系统 - 前台运行
echo.
echo ============================================
echo   流媒体自动录制系统 v2.3
echo   前台运行模式 (Ctrl+C 停止)
echo ============================================
echo.
python main.py
