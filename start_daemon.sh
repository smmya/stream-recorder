#!/bin/bash
# 流媒体自动录制系统 - 后台启动脚本 (Linux/macOS)
cd "$(dirname "$0")"
echo "============================================"
echo "  流媒体自动录制系统 v2.4"
echo "  后台模式启动中..."
echo "  PID 文件: ./logs/server.pid"
echo "============================================"
python3 main.py --daemon
