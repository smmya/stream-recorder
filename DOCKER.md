# Docker 部署指南

## 前置要求

- Docker 20.10+
- Docker Compose 2.0+（可选，推荐）

---

## 一、构建镜像

### 方式 A：docker build

```bash
cd stream-recorder
docker build -t stream-recorder:latest .
```

构建完成后验证：

```bash
docker images stream-recorder
```

### 方式 B：docker compose build

```bash
cd stream-recorder
docker compose build
```

---

## 二、运行容器

### 2.1 快速启动（docker run）

```bash
docker run -d \
  --name stream-recorder \
  -p 5000:5000 \
  -v $(pwd)/recordings:/app/recordings \
  -v $(pwd)/config.yaml:/app/config.yaml \
  -v $(pwd)/logs:/app/logs \
  stream-recorder:latest --test
```

> `--test` 使容器监听 0.0.0.0，否则 Docker 端口映射会失效。

### 2.2 Compose 启动（推荐）

```bash
docker compose up -d
```

---

## 三、参数说明

挂载到容器的 `--test` 等价于：

```
python main.py --test
```

其他可用参数：

| 参数 | 作用 |
|------|------|
| `--test` | 监听 0.0.0.0（Docker 必须） |
| `--port 8080` | 自定义端口 |
| `--host 0.0.0.0` | 精确指定监听地址 |

---

## 四、日常管理

```bash
# 查看日志
docker logs -f stream-recorder

# 查看状态
docker ps | grep stream-recorder

# 重启
docker restart stream-recorder

# 停止
docker stop stream-recorder

# 删除容器（数据在 volumes 中不受影响）
docker rm stream-recorder

# 使用 compose
docker compose down          # 停止
docker compose up -d         # 启动
docker compose restart       # 重启
docker compose logs -f       # 日志
```

---

## 五、端口映射

默认映射 `5000:5000`。改为其他端口：

```bash
# docker run
docker run -d -p 8080:5000 ... stream-recorder:latest --test

# docker compose（修改 docker-compose.yml）
ports:
  - "8080:5000"
```

访问：`http://服务器IP:8080`

---

## 六、配置修改

**方法 1**：直接编辑宿主机 `config.yaml`，重启容器生效：

```bash
vim config.yaml
docker restart stream-recorder
```

**方法 2**：进入容器修改：

```bash
docker exec -it stream-recorder vim /app/config.yaml
docker restart stream-recorder
```

---

## 七、数据持久化

| 目录 | 用途 | 持久化 |
|------|------|--------|
| `/app/recordings` | MP4 录制文件 | ✅ Volume |
| `/app/config.yaml` | 配置文件 | ✅ Volume |
| `/app/logs` | 运行日志 | ✅ Volume（仅 daemon 模式） |

---

## 八、完整部署流程（新服务器）

```bash
# 1. 上传项目到服务器
scp -r stream-recorder user@server:/opt/

# 2. 登录服务器
ssh user@server

# 3. 进入项目目录
cd /opt/stream-recorder

# 4. 修改默认账号密码
vim config.yaml
# 修改 auth.username 和 auth.password

# 5. 构建并启动
docker compose up -d --build

# 6. 验证
curl http://127.0.0.1:5000/api/check_auth

# 7. 配置 Nginx 反代（可选）
# 参见下方 Nginx 反代章节
```

---

## 九、Nginx 反向代理（可选）

```nginx
server {
    listen 80;
    server_name your-domain.com;

    # 上传大小限制（视频文件可能很大）
    client_max_body_size 0;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_buffering off;                       # 视频流不要缓冲
        proxy_read_timeout 3600s;                  # 视频播放超时
    }
}
```

---

## 十、镜像大小优化（可选）

如需更小的镜像，用下面替换 Dockerfile 的 FROM 行：

```dockerfile
FROM python:3.13-alpine
RUN apk add --no-cache ffmpeg
```

Alpine 镜像约 200MB（slim 约 300MB），但注意 Alpine 的 FFmpeg 功能可能缺少部分编解码器。
