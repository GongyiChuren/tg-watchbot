<div align="center">
  <h1>tg-watchbot</h1>
  <p>Telegram 双向客服机器人 + Web/RSS 监控推送 + 可视化管理面板</p>
  <p>双向对话 · 关键词监控 · 私聊广告拦截 · 多管理员 · 配置导入导出</p>
  <p>
    <a href="#ai-one-line-install">AI 一句话安装</a> ·
    <a href="#docker-install">Docker 安装</a> ·
    <a href="#manual-install">手动安装</a> ·
    <a href="#systemd-install">systemd 部署</a> ·
    <a href="#面板路由">面板路由</a> ·
    <a href="#更新日志">更新日志</a>
  </p>
</div>

## 简介：
tg-watchbot 是一个轻量级 Python 服务，把 **Telegram 双向客服机器人** 和 **Web/RSS 监控推送** 合在一起：

- 普通用户私聊 Bot，消息会转发给管理员；
- 管理员可以直接回复、主动发文字/图片、封禁/备注用户；
- 后台定时监控 RSS 或网页，命中关键词、新条目、价格/库存变化后推送给管理员；
- 自带一个 Web 管理面板，可配置监控目标、编辑 YAML、查看收件箱和日志。

项目为单文件应用，适合个人服务器、NAT 小鸡、轻量 VPS 直接用 systemd 跑。
<a id="ai-one-line-install"></a>

## AI 一句话安装
```
请克隆 `https://github.com/GongyiChuren/tg-watchbot.git` 到本机，默认使用 Docker 按 README 的 `Docker 安装（含自启）` 启动服务；若无 Docker，则按 `快速开始` 走 Python 直接安装。启动后打开 `http://127.0.0.1:8765`，在面板填写 `TELEGRAM_BOT_TOKEN` 和 `ADMIN_CHAT_ID`，保存后执行重启（Docker 用 `docker compose restart`，直接安装用重启进程）。
``` 
## 更新日志

### 2026-05-21 第二次更新

- Web 面板新增收件箱直接回复、用户管理、快捷回复、私聊广告拦截、监控推送历史、配置导入/导出。
- 收件箱改为完整双向对话记录：用户消息、Web 回复、TG 管理员回复都会显示。
- 用户管理页新增 Bot / 面板配置卡片，和设置页共用同一份配置；修改 Token、管理员 ID、端口、账号或密码后需要重启。
- `ADMIN_CHAT_ID` 支持最多 3 个管理员，用逗号分隔。
- 单个监控可关闭 Telegram 推送，只记录到 Web 推送历史。

### 2026-05-21 第一次更新

- 默认启动改为先启动 Web 面板：未填写 `TELEGRAM_BOT_TOKEN` / `ADMIN_CHAT_ID` 时，面板仍可打开，同时 Telegram 收发、监控推送不可用。
- 面板配置页可填写 Bot Token、管理员 ID、面板账号和清理策略；保存后需要重启服务让 Bot 配置生效。
- 修复到期消息删除：监控推送消息支持到期自动删除，默认 `60` 分钟。
- 保存配置时会保留 `WEB_PANEL_SESSION_SECRET`，避免保存后登录状态被重置。
- Web 面板界面和站点图标已更新优化。

## 功能

### Telegram 双向机器人

- 使用官方 Telegram Bot API，不做 userbot/selfbot。
- `/start` 建立用户和管理员之间的联系。
- 用户消息先写入 SQLite，再转发给管理员，避免转发失败时丢消息。
- 管理员可通过“回复转发消息”直接回给原用户。
- 支持显式命令：
  - `/reply <user_id> <内容>`：给指定用户发文字；
  - `/sendpic <user_id> [说明]`：给指定用户发图片；
  - `/block <user_id>`：封禁用户；
  - `/unblock <user_id>`：解封用户；
  - `/note <user_id> <备注>`：给用户加备注；
  - `/who <user_id>`：查看用户信息；
  - `/spamwords`：查看广告关键词；
  - `/spamadd <关键词>`：添加广告关键词；
  - `/spamdel <关键词>`：删除广告关键词；
  - `/cancel`：取消待发送图片。
- 普通用户有简单限流，防止刷屏。
- 支持最多 3 个管理员 chat id，用逗号分隔配置。
- 支持私聊广告关键词自动拦截和自动拉黑，不影响 RSS/Web 监控。

![示例图片](https://pic.gongyichuren.de/file/1779287173835_8521cab29a9635743a603582ceb7ba02.png)

### Web/RSS 监控

- 支持两类监控：
  - `rss`：解析 RSS/Atom 条目；
  - `web`：用 CSS selector 抓网页条目、标题、链接、价格、库存。
- 支持触发条件：
  - 关键词命中；
  - 新条目；
  - 价格变化；
  - 库存变化。
- 支持论坛 RSS 增强字段：作者、分类、tags、摘要。
- 支持去重，避免同一条反复推送。
- 支持屏蔽词、作者、分类过滤（YAML 高级配置）。
- 单个监控可关闭 Telegram 推送，只记录到 Web 推送历史。
- 默认最低监控间隔为 60 秒。

![示例图片](https://pic.gongyichuren.de/file/1779287170665_17b7c8b4040d6334ea62a108d08db644.png)

### Web 管理面板

- 登录页 + HttpOnly session cookie，不使用丑陋的浏览器 Basic Auth。
- 监控列表、新增、编辑、删除、手动检查、预览。
- NodeSeek / Linux.do RSS 模板。
- 批量新增监控。
- YAML 高级编辑。
- Bot Token / 管理员 ID / 面板账号配置页。
- 收件箱页面，可查看完整双向对话记录、重试转发、直接回复。
- 用户管理页，可备注、封禁、解封、主动发消息，并可编辑 Bot / 面板配置。
- 私聊广告拦截规则和快捷回复模板可在 Web 面板编辑。
- 监控推送历史页，可查看 Telegram 推送和仅 Web 记录。
- `config.yaml` 导入/导出页面，方便迁移。
- 主动发消息页面 `/send`，发送成功后会在页面显示结果，并给管理员聊天发送确认提醒。
- 自动清理监控/RSS/网站状态数据；支持定时删除 Telegram 监控通知消息；不会删除用户、收件箱、双向对话消息。
- 日志页面和健康检查 `/health`。

![示例图片](https://pic.gongyichuren.de/file/1779345259571_image.png)

## 使用的开源库

本项目的业务逻辑为自写，主要使用并参考了以下开源库的公开 API 和常见用法：

- [`aiogram`](https://github.com/aiogram/aiogram)：Telegram Bot API、命令、消息处理、复制/发送消息。
- [`FastAPI`](https://github.com/fastapi/fastapi)：Web 管理面板、表单、路由、中间件。
- [`Uvicorn`](https://github.com/encode/uvicorn)：ASGI 服务运行。
- [`APScheduler`](https://github.com/agronholm/apscheduler)：异步定时监控任务。
- [`httpx`](https://github.com/encode/httpx)：异步 HTTP 抓取。
- [`feedparser`](https://github.com/kurtmckee/feedparser)：RSS/Atom 解析。
- [`Beautiful Soup`](https://www.crummy.com/software/BeautifulSoup/)：HTML 解析和 CSS selector 抽取。
- [`PyYAML`](https://pyyaml.org/)：`config.yaml` 配置读写。
- [`python-dotenv`](https://github.com/theskumar/python-dotenv)：读取 `.env`。
- Python 标准库 `sqlite3`：消息、用户、去重、监控状态持久化。

## 安全说明

- 如果要把面板暴露到公网，建议使用 Cloudflare Access / 反代鉴权，并使用强密码。
- Bot 只能给“已经主动私聊过 Bot 的用户”发消息，这是 Telegram Bot API 的限制。

<a id="manual-install"></a>
## 快速开始

<a id="docker-install"></a>
## Docker 安装（含自启）

```bash
git clone https://github.com/GongyiChuren/tg-watchbot.git tg-watchbot
cd tg-watchbot
cp .env.example .env
cp config.example.yaml config.yaml
touch tg-watchbot.sqlite3 tg-watchbot.log
docker compose up -d --build
```

查看状态与日志：

```bash
docker compose ps
docker compose logs -f
```

修改配置后重启：

```bash
docker compose restart
```

```bash
git clone https://github.com/GongyiChuren/tg-watchbot.git tg-watchbot
cd tg-watchbot
python3 -m venv .venv
./.venv/bin/pip install -U pip
./.venv/bin/pip install -r requirements.txt
cp .env.example .env
cp config.example.yaml config.yaml
```

启动：

```bash
./.venv/bin/python app.py
```

打开面板：

```text
http://127.0.0.1:8765
```

默认账号来自 `.env.example`：

```text
用户名：admin
密码：change-me
```

登录后进入“设置”，填写 Bot Token、管理员 Telegram 数字 chat id、面板账号和密码。保存后重启服务，Bot 才会开始收发 Telegram 消息和发送监控通知。

手动跑一次监控：

```bash
./.venv/bin/python app.py --run-once
```

<a id="systemd-install"></a>
## systemd 部署

推荐部署到 `/opt/tg-watchbot`：

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin tg-watchbot || true
sudo mkdir -p /opt/tg-watchbot
sudo chown -R "$USER:$USER" /opt/tg-watchbot

cd /opt/tg-watchbot
git clone https://github.com/GongyiChuren/tg-watchbot.git .
python3 -m venv .venv
./.venv/bin/pip install -U pip
./.venv/bin/pip install -r requirements.txt
cp .env.example .env
cp config.example.yaml config.yaml

# 先用前台模式打开面板，确认能登录和保存配置
./.venv/bin/python app.py
```

在服务器本机打开：

```text
http://127.0.0.1:8765
```

默认账号来自 `.env.example`：

```text
用户名：admin
密码：change-me
```

如果要从公网访问面板，推荐用 Cloudflare Tunnel + Zero Trust Access（需要域名），不需要开放服务器入站端口，也不用把 `WEB_PANEL_HOST` 改成 `0.0.0.0`。

基本步骤：

1. 在 Cloudflare Zero Trust 后台进入 `Networks` -> `Tunnels`，创建一个 Cloudflared Tunnel。
2. 按页面提示在服务器安装并启动 `cloudflared`。
3. 添加 Public Hostname，例如 `tg.example.com`。
4. Service 填：

```text
http://127.0.0.1:8765
```

5. 在 Zero Trust 的 `Access` 里给这个域名加登录策略，例如只允许自己的邮箱访问。

临时调试也可以用 SSH 端口转发：

```bash
ssh -L 8765:127.0.0.1:8765 user@服务器IP
```

然后在自己电脑打开 `http://127.0.0.1:8765`。

在面板“设置”里填好 Bot Token、管理员 ID、面板账号和密码后，停止前台进程，再安装 systemd 服务：

```bash
sudo chown -R tg-watchbot:tg-watchbot /opt/tg-watchbot
sudo chmod 600 /opt/tg-watchbot/.env
sudo cp systemd/tg-watchbot.service /etc/systemd/system/tg-watchbot.service
sudo systemctl daemon-reload
sudo systemctl enable --now tg-watchbot
sudo journalctl -u tg-watchbot -f
```

健康检查：

```bash
curl http://127.0.0.1:8765/health
```

说明：`/restart` 命令在 systemd 下会让进程退出，由 `Restart=on-failure` 自动拉起；如果是手动 `python app.py` 启动，退出后需要自己重新执行启动命令。

## 配置说明

### `.env`

| 变量 | 说明 |
|---|---|
| `TELEGRAM_BOT_TOKEN` | BotFather 创建的 Telegram Bot Token |
| `ADMIN_CHAT_ID` | 管理员 Telegram 数字 chat id；最多 3 个，用逗号分隔 |
| `LOG_LEVEL` | 日志级别，默认 `INFO` |
| `WEB_PANEL_ENABLED` | 是否启用 Web 面板，默认 `true` |
| `WEB_PANEL_HOST` | 面板监听地址，默认 `127.0.0.1` |
| `WEB_PANEL_PORT` | 面板端口，默认 `8765` |
| `WEB_PANEL_USER` | 面板用户名 |
| `WEB_PANEL_PASSWORD` | 面板密码 |
| `WEB_PANEL_SESSION_SECRET` | Session Secret，留空会自动生成并写回 `.env` |

### `config.yaml`

Bot 扩展配置示例：

```yaml
bot:
  rate_limit:
    window_seconds: 10
    max_messages: 3
  spam_filter:
    enabled: true
    auto_block: true
    keywords:
      - 投资
      - 博彩
      - 空投
  quick_replies:
    - title: 已收到
      text: 你好，消息已收到，我稍后处理。
```

监控数据自动清理示例：

```yaml
cleanup:
  enabled: true
  interval_minutes: 60              # 每多少分钟执行一次清理
  monitor_message_delete_after_minutes: 60  # 监控通知消息发送后多久删除；0 表示不删除
  monitor_retention_minutes: 1440   # RSS/网站监控状态保留多久
```

清理范围只包括：

- `monitor_state`：网站/RSS 条目状态、价格/库存状态；
- `sent_events`：监控推送去重记录；
- `monitor_messages`：等待到期删除的 Telegram 监控通知消息队列。

不会删除：

- `users`；
- `message_map`；
- `inbox_messages`；
- 任何双向对话/客服消息记录。

RSS 示例：

```yaml
monitors:
  - name: NodeSeek 新帖
    type: rss
    url: https://rss.nodeseek.com/
    interval_seconds: 60
    keywords:
      - VPS
      - 优惠
    exclude_keywords:
      - 出号
    authors: []
    categories: []
    notify_on:
      keyword_match: true
      new_item: true
      price_change: false
      stock_change: false
    notify_telegram: true
    forum: true
```

网页示例：

```yaml
monitors:
  - name: Example Deals
    type: web
    url: https://example.com/deals
    interval_seconds: 300
    keywords:
      - discount
    selectors:
      item: article, .deal, li
      title: h1, h2, h3, a
      link: a
      price: .price
      stock: .stock
    notify_on:
      keyword_match: true
      new_item: true
      price_change: true
      stock_change: true
    notify_telegram: true
```

## 管理命令

管理员在 Telegram 里可用：

```text
/reply <user_id> <内容>
/sendpic <user_id> [图片说明]
/block <user_id>
/unblock <user_id>
/note <user_id> <备注>
/who <user_id>
/spamwords
/spamadd <关键词>
/spamdel <关键词>
/cancel
```

也可以直接“回复 Bot 转发给管理员的用户消息”，Bot 会按映射把回复发回原用户。

## 面板路由

| 路由 | 说明 |
|---|---|
| `/` | 监控列表 |
| `/monitor/new` | 新增监控 |
| `/monitor/templates` | 论坛模板 |
| `/monitor/bulk` | 批量新增 |
| `/monitor/{idx}/preview` | 预览抓取结果，不写入状态、不推送 |
| `/monitor/{idx}/run` | 手动检查单个监控 |
| `/run-once` | 手动检查全部监控 |
| `/yaml` | YAML 高级编辑 |
| `/settings` | `.env` 设置和监控清理策略 |
| `/send` | 主动发消息给已私聊过 Bot 的用户 |
| `/inbox` | 收件箱 |
| `/users` | 用户管理 |
| `/rules` | 私聊广告拦截规则 |
| `/replies` | 快捷回复模板 |
| `/monitor/events` | 监控推送历史 |
| `/config/export` | 导出 / 导入 `config.yaml` |
| `/logs` | 日志 |
| `/health` | 健康检查 |

## 注意事项

- Telegram Bot 不能主动私聊陌生人；对方必须先给 Bot 发过 `/start` 或任意消息。
- 对公网暴露 Web 面板前，务必改默认密码。
- RSS 监控建议 60 秒起步；网页监控建议更保守，避免对目标站造成压力。
- 媒体消息当前只保证记录文本/说明和转发状态；转发失败后的媒体补发需要额外做本地附件存储。

## License

本项目采用非商业授权。

你可以：
- 学习、研究、个人使用本项目
- 修改代码用于非商业用途
- 在非商业项目中使用本项目

你必须：
- 保留原作者署名
- 在引用或二次发布时注明项目来源：
  https://github.com/GongyiChuren/tg-watchbot

你不可以：
- 将本项目或其修改版本用于商业用途
- 售卖本项目或基于本项目提供付费服务
- 在未获得作者书面许可的情况下用于商业产品

商业使用请先联系作者获得授权。
