FROM python:3.13-slim

# 安装 FFmpeg（用于录制和 RTMP 探测）
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY main.py .
COPY config.yaml .
COPY static/ ./static/

# 创建必要目录
RUN mkdir -p recordings logs

# 默认仅监听本地（安全），使用 --test 或 -p 5000 映射端口对外开放
EXPOSE 5000

# 前台模式运行（Docker 容器需要前台进程）
ENTRYPOINT ["python", "main.py"]
CMD []
