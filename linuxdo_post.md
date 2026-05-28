# Telegram机器人：双向对话 Bot + 网页关键词推送 + 可视化面板 + tg群消息管理 + 广告屏蔽
## 资源荟萃

本帖使用社区开源推广，符合推广要求。我申明并遵循社区要求的以下内容：

- 我的帖子已经打上 `#开源推广` 标签：是
- 我的开源项目完整开源，无未开源部分：是
- 我的开源项目已链接认可 LINUX DO 社区：是
- 我帖子内的项目介绍，AI生成、润色内容部分已截图发出：是
- 以上选择我承诺是永久有效的，接受社区和佬友监督：是

以下为项目介绍正文内容，AI 生成、润色内容已使用截图方式发出。

## 更新日志

### 2026-05-22 更新
- TG 群监听功能增强：支持可视化配置监听规则、AI 总结参数与防刷屏策略。
- TG 群监听新增“已发现群聊”：自动显示 Bot 收到过消息的群聊 `chat_id`，可一键创建监听。
- 新增 `/update` 安全更新流程：显示本地/远端 commit、ahead/behind、工作区状态；仅允许 `ff-only` 更新。
- 更新前若检测到本地未提交改动，会拒绝更新；避免覆盖本地代码。
- 新增“回滚上次更新”按钮：更新前自动记录回滚点，可一键回滚并重启。
- TG 群监听 AI 总结新增可视化高级控制：`ai_prompt`、`ai_min_interval_seconds`、`ai_dedupe_window_seconds`。
- TG 群监听增加限频和去重窗口，降低重复推送与 AI 调用成本；AI 失败时仍会回退模板摘要。
- 监控面板新增可观测状态：最近成功/失败时间、最近错误、耗时、推送数、连续失败次数。

### 2026-05-21 第二次更新
- Web 面板新增收件箱直接回复、用户管理、快捷回复、私聊广告拦截、监控推送历史、配置导入/导出。
- 收件箱改为完整双向对话记录：用户消息、Web 回复、TG 管理员回复都会显示。
- 用户管理页新增 Bot / 面板配置卡片，和设置页共用同一份配置；修改 Token、管理员 ID、端口、账号或密码后需要重启。
- `ADMIN_CHAT_ID` 支持最多 3 个管理员，用逗号分隔。
- 单个监控可关闭 Telegram 推送，只记录到 Web 推送历史。

### 2026-05-21
- 默认启动改为先启动 Web 面板：未填写 `TELEGRAM_BOT_TOKEN` / `ADMIN_CHAT_ID` 时，面板仍可打开，同时 Telegram 收发、监控推送不可用。
- 面板配置页可填写 Bot Token、管理员 ID、面板账号和清理策略；保存后需要重启服务让 Bot 配置生效。
- 修复到期消息无法删除的 bug：监控推送消息支持到期自动删除，默认 60 分钟。
- 保存配置时会保留 `WEB_PANEL_SESSION_SECRET`，避免保存后登录状态被重置。
- Web 面板界面和站点图标已更新优化。

（新版安装方法详见项目地址）

最近看 TG 上不少人说封号比较严重。再加上我作为一个刚入门的 MJJ，平时也想更快地获取信息，所以 vibe 了一个自用小工具：`tg-watchbot`。

它是一个轻量级 Python 服务，把 Telegram 双向对话 Bot 和 Web/RSS 监控推送 合在一起，并提供网页端可视化管理面板。它适合放在 VPS、NAT 小鸡或者家里的小服务器上跑。

项目地址：

[GitHub - GongyiChuren/tg-watchbot](https://github.com/GongyiChuren/tg-watchbot)

## 为什么做这个

我最开始的需求很简单：

- 有一个自己的 Telegram 联系入口，别人私聊 Bot 后可以转发到管理员账号；
- 能实时获取 Linux.do、博客、商店页面等信息源；
- 命中关键词、新条目、价格变化或库存变化时，自动推送到 Telegram；
- 有网页面板，不用每次都手改配置文件。

另外，项目支持定时清理监控推送相关状态数据，避免网页监控信息过多，影响正常的双向对话使用。

## 主要功能

- 普通用户私聊 Bot，消息会转发给管理员
- 管理员可以直接回复用户，也可以主动发文字 / 图片
- 支持封禁、解封、备注、查看用户信息
- 支持 RSS / Atom 监控
- 支持网页 CSS selector 抓取
- 支持关键词、新条目、价格变化、库存变化提醒
- 内置 Web 管理面板
- 可以在面板里新增、编辑、删除监控
- 支持 RSS 模板
- 使用 SQLite 保存用户、消息、监控状态
- 支持 systemd 部署

## 展示

> 这里放截图

## AI 一句话安装

请克隆 `https://github.com/GongyiChuren/tg-watchbot.git` 到本机，默认使用 Docker 按 README 的 `Docker 安装（含自启）` 启动服务；若无 Docker，则按 `快速开始` 走 Python 和 systemd 部署直接安装。启动后打开 `http://127.0.0.1:8765`，安装完成后提醒用户记得在面板填写 `TELEGRAM_BOT_TOKEN` 和 `ADMIN_CHAT_ID` 并在之后保存并执行重启（Docker 用 `docker compose restart`，直接安装用重启进程）。

## Docker 安装（含自启）

```bash
git clone https://github.com/GongyiChuren/tg-watchbot.git tg-watchbot
cd tg-watchbot
cp .env.example .env
cp config.example.yaml config.yaml
touch tg-watchbot.sqlite3 tg-watchbot.log
docker compose up -d --build
```

Docker 会在容器内监听 `0.0.0.0:8765`，宿主机仍然打开 `http://127.0.0.1:8765`。

查看状态与日志：

```bash
docker compose ps
docker compose logs -f
```

修改配置后重启：

```bash
docker compose restart
```

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

`http://127.0.0.1:8765`

默认账号来自 `.env.example`：

- 用户名：admin
- 密码：change-me

## 注意事项

- Telegram Bot 不能主动私聊陌生人，对方必须先给 Bot 发过消息
- `.env` 里有 Token 和密码，不要提交到 GitHub
- Web 面板如果暴露到公网，建议套 Cloudflare Access / 反代鉴权
- RSS 监控建议 60 秒起步
- 网页监控建议更保守一点，避免对目标站造成压力

目前它还是一个自用小工具，目标是够轻、够直接、够容易部署。后续会继续修 bug 😋

可以的话点个 star 吧，谢谢佬们🙏🙏🙏
