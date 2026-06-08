# 🎬 流媒体自动录制系统

基于 Python FastAPI 的流媒体自动录制管理平台。支持 SRS / nginx-rtmp API 多源轮询，以及 RTMP/RTMPS 直接订阅，通过 FFmpeg 自动录制为 MP4 文件。

---

## 功能特性

### 流源管理
- **API 模式**：轮询 SRS HTTP API 或 nginx-rtmp stat 页面，自动发现直播流并录制
- **RTMP 直接订阅**：单个 RTMP/RTMPS 地址即一个独立任务，无需依赖流媒体服务器 API
- **直播探测**：使用 ffprobe 检测 RTMP 流是否在线（非盲目连接）
- **时间段控制**：每个订阅可独立设置轮询起止时间，避免无效探测
- **单流独立间隔**：每个 RTMP 订阅可自定义轮询间隔（秒）

### 录制引擎
- **自动录制**：检测到流在线 → FFmpeg 自动开始录制；流下线 → 自动停止
- **去重保护**：录制期间该流自动暂停轮询，杜绝重复录制
- **录制恢复**：FFmpeg 异常退出后，若流仍在线则自动重连录制
- **备注命名**：订阅备注体现在文件名中（如 `前门摄像头_20260608_163000.mp4`）

### 脚本钩子
- **全局脚本**：录制开始/结束时执行自定义 Shell 命令
- **专用脚本**：每个 RTMP 订阅可单独配置钩子，优先级高于全局
- **变量替换**：支持 `{stream_name}` `{filename}` `{start_time}` 等动态参数
- **异步执行**：钩子后台运行，不阻塞录制流程

### 安全
- **JWT 认证**：基于配置文件的账号密码登录
- **TOTP 二次验证**：支持 Google / Microsoft Authenticator，零信任登录
- **IP 自动封禁**：连续 N 次登录失败后自动封禁 M 分钟
- **封禁管理**：Web 界面查看/手动解封被封 IP
- **安全监听**：默认仅监听 127.0.0.1，`--test` 参数切换为 0.0.0.0

### 管理界面
- **暗色主题** Web UI，单页应用（SPA）
- **仪表盘**：实时统计流源数、活跃流数、录制数、文件数
- **录制文件管理**：列表查看、在线播放（拖拽进度条）、一键删除
- **可视化文件夹选择器**：设置页浏览文件系统选择录制目录，一键测试读写
- **端口配置**：Web 界面修改监听端口（重启生效）

### 部署
- **双模式启动**：前台模式（supervisor/systemd） / 后台模式（手动守护）
- **Docker 支持**：Dockerfile + docker-compose.yml 一键部署
- **Linux systemd 服务**：开箱即用的 service 文件
- **跨平台**：Windows / Linux 均可运行

---

## 快速开始

### 本地运行

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 修改默认密码
vim config.yaml   # 修改 auth.username 和 auth.password

# 3. 启动（前台模式）
python main.py

# 访问 http://127.0.0.1:5000
# 账号: admin / admin123（默认）
```

### Docker 部署

```bash
docker compose up -d --build
```

详细说明见 [DOCKER.md](DOCKER.md)。

---

## 启动参数

```
python main.py [--daemon] [--test] [--host IP] [--port PORT]
```

| 参数 | 说明 |
|------|------|
| `--daemon`, `-d` | 后台模式，进程脱离终端，日志写入 `logs/` |
| `--test`, `-t` | 监听 0.0.0.0（允许外部访问），默认仅 127.0.0.1 |
| `--host IP` | 指定监听地址（最高优先级，覆盖 --test 和配置） |
| `--port PORT`, `-p` | 指定监听端口（最高优先级，覆盖配置和默认值） |

**优先级**：命令行参数 > Web 设置页 > config.yaml > 默认值

**便捷脚本**：
- Windows：双击 `start_foreground.bat` / `start_daemon.bat`
- Linux：`bash start_foreground.sh` / `bash start_daemon.sh`

---

## 配置文件

`config.yaml` 完整示例：

```yaml
# 登录认证
auth:
  username: admin
  password: admin123

# 安全配置
security:
  max_failed_attempts: 5        # IP 最大失败次数
  ban_duration_minutes: 15      # 封禁时长（分钟）
  totp_enabled: false           # 全局 TOTP 开关

# 全局脚本钩子
hooks:
  on_recording_start: ""        # 录制开始时执行的命令
  on_recording_stop: ""         # 录制停止时执行的命令
  # 变量: {stream_name} {source_url} {filename} {start_time} {stop_time} {remarks}

# 流源（SRS / nginx-rtmp API 模式）
stream_sources:
  - name: "SRS 服务器"
    type: srs
    api_url: "http://192.168.1.100:1985/api/v1/streams"
    rtmp_base: "rtmp://192.168.1.100/live/"

  - name: "Nginx-RTMP 服务器"
    type: nginx-rtmp
    api_url: "http://192.168.1.101:8080/stat"
    rtmp_base: "rtmp://192.168.1.101/live/"

# RTMP 直接订阅
rtmp_subscriptions:
  - url: "rtmp://192.168.1.100/live/cam1"
    remarks: "前门摄像头"
    enabled: true
    poll_start: "08:00"
    poll_end: "22:00"
    poll_interval: 30          # 独立轮询间隔（秒）
    probe_timeout: 5
    hooks:                      # 专用脚本（优先于全局）
      on_start: "curl -X POST http://api/notify?stream={stream_name}&action=start"
      on_stop: "python /scripts/upload.py --file {output_file}"

# 录制
recording:
  output_dir: "./recordings"
  ffmpeg_path: "ffmpeg"
  ffprobe_path: "ffprobe"
  ffmpeg_args: "-c copy -f mp4"

# 服务
server:
  host: "127.0.0.1"
  port: 5000
  secret_key: "change-me-to-a-random-string"
```

---

## 脚本钩子变量

录制脚本中可用的变量占位符：

| 变量 | 含义 | 示例值 |
|------|------|--------|
| `{stream_name}` | 流名称/URL | `rtmp://192.168.1.100/live/cam1` |
| `{source_url}` | RTMP 源地址 | `rtmp://192.168.1.100/live/cam1` |
| `{source_name}` | 流源名称 | `前门摄像头` |
| `{filename}` | 输出文件名 | `前门摄像头_20260608_163000.mp4` |
| `{output_file}` | 输出完整路径 | `/data/recordings/前门摄像头_20260608_163000.mp4` |
| `{remarks}` | 订阅备注 | `前门摄像头` |
| `{start_time}` | 录制开始时间 | `2026-06-08 16:30:00` |
| `{stop_time}` | 录制停止时间 | `2026-06-08 16:35:00` |
| `{record_type}` | 录制类型 | `rtmp_sub` 或 `api` |

---

## Linux systemd 部署

```bash
# 1. 复制项目
sudo cp -r stream-recorder /opt/

# 2. 安装服务
sudo cp /opt/stream-recorder/stream-recorder.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now stream-recorder

# 3. 日常管理
systemctl status stream-recorder
systemctl restart stream-recorder
journalctl -u stream-recorder -f
```

---

## 目录结构

```
stream-recorder/
├── main.py                  # 后端入口 (FastAPI)
├── config.yaml              # 配置文件
├── requirements.txt         # Python 依赖
├── Dockerfile               # Docker 镜像
├── docker-compose.yml       # Docker Compose 编排
├── DOCKER.md                # Docker 部署文档
├── .dockerignore
├── stream-recorder.service  # systemd 服务文件
├── start_foreground.bat     # Windows 前台启动
├── start_daemon.bat         # Windows 后台启动
├── start_foreground.sh      # Linux 前台启动
├── start_daemon.sh          # Linux 后台启动
├── static/
│   └── index.html           # 前端 SPA
├── recordings/              # 录制文件目录
└── logs/                    # 后台模式日志
```

---

## API 接口

> 以下接口均需登录（Cookie 或 Bearer Token）

### 认证
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/login` | 登录，body: `{username, password, totp_code?}` |
| GET  | `/api/logout` | 登出 |
| GET  | `/api/check_auth` | 检查登录状态 |

### 流源管理（API 模式）
| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/sources` | 获取流源列表 |
| POST | `/api/sources` | 添加流源 |
| PUT  | `/api/sources/{id}` | 更新流源 |
| DELETE | `/api/sources/{id}` | 删除流源 |

### RTMP 订阅
| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/subscriptions` | 获取订阅列表 |
| POST | `/api/subscriptions` | 添加订阅 |
| PUT  | `/api/subscriptions/{id}` | 更新订阅 |
| DELETE | `/api/subscriptions/{id}` | 删除订阅 |
| POST | `/api/subscriptions/{id}/probe` | 手动探测订阅 |

### 轮询控制
| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/polling/status` | 轮询状态 |
| POST | `/api/polling/start` | 启动轮询 |
| POST | `/api/polling/stop` | 停止轮询 |
| PUT  | `/api/polling/interval` | 修改全局轮询间隔 |

### 录制文件
| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/recordings` | 录制文件列表 |
| DELETE | `/api/recordings/{filename}` | 删除文件 |
| GET  | `/api/video/{filename}` | 流式播放（支持 Range） |

### 安全
| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/security/status` | 安全状态（IP、封禁列表） |
| POST | `/api/security/totp/setup` | 生成 TOTP 密钥和二维码 |
| POST | `/api/security/totp/verify` | 验证并启用 TOTP |
| POST | `/api/security/totp/disable` | 禁用 TOTP |
| POST | `/api/security/unban` | 解封 IP |

### 系统配置
| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/config` | 获取当前配置 |
| PUT  | `/api/config/recording_dir` | 设置录制目录 |
| PUT  | `/api/config/port` | 设置 Web 端口 |
| GET  | `/api/filesystem/browse` | 浏览文件系统目录 |
| POST | `/api/filesystem/test` | 测试目录读写 |
| PUT  | `/api/config/auth` | 修改账号密码 |

### 实时状态
| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/streams/status` | 所有流实时状态 |

---

## 常见问题

**Q: 为什么添加的 RTMP 订阅一直显示离线？**  
A: 确认 RTMP 地址格式正确（`rtmp://ip/app/stream`），且 ffprobe 可正常访问该流。可点击「探测」按钮手动测试。

**Q: 端口修改后没有生效？**  
A: 端口修改需重启服务才能生效。设置页有明确提示。

**Q: 如何让服务开机自启？**  
A: Linux 使用 systemd（见上方部署章节），Windows 可将 `start_daemon.bat` 加入任务计划程序。

**Q: Docker 容器内录制文件如何持久化？**  
A: `docker-compose.yml` 已将 `recordings/` 映射为 volume，文件保存在宿主机，删除容器不会丢失。

**Q: 如何对接钉钉/企业微信通知？**  
A: 使用脚本钩子功能。在订阅的 `hooks.on_start` 中填写 webhook URL，如：  
`curl -X POST https://oapi.dingtalk.com/robot/send?access_token=xxx -H 'Content-Type: application/json' -d '{"msgtype":"text","text":{"content":"开始录制: {filename}"}}'`
