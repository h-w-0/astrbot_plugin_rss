# AstrBot RSS 主动推送插件

一个支持群白名单的 RSS 主动推送插件。定时获取 RSS Feed 的新条目，自动推送到指定 QQ 群。

## 功能

- 🔄 定时轮询 RSS，自动发现新条目并推送
- 👥 群聊白名单机制，只有白名单中的群才会收到推送
- 📝 自定义推送消息标题
- ⏱️ 可配置检查间隔
- 🔒 已推送条目自动去重（基于 GUID），不会重复推送
- 🛑 `/rss_stop` 一键跳过当前未推送的条目，之后的新帖子仍正常推送
- 🔄 `/rss_reset` 清空已推送记录，重新推送所有条目

## 安装

### 方式一：直接放入插件目录

1. 将本插件文件夹 `astrbot_plugin_rss_push` 放入 AstrBot 的 `data/plugins/` 目录
2. 在 AstrBot WebUI 的「插件管理」中点击「重载插件」
3. 插件会自动安装依赖（`feedparser`、`aiohttp`）

## 配置文件

在 AstrBot WebUI → 插件管理 → 点击本插件 → 配置，可看到以下配置项：

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `rss_url` | string | `https://lostmedia.wikidot.com/feed/forum/posts.xml` | RSS 订阅源地址 |
| `check_interval` | int | `300` | 检查间隔（秒） |
| `rss_title` | string | `失传媒体中文维基 论坛新消息` | 推送消息的标题行 |
| `platform` | string | `default` | 消息平台前缀，发 `/rss_debug_origin` 查看你的实际值 |
| `group_whitelist` | list | `[]` | 推送群聊白名单（群号列表） |

## 指令

在群内发送以下指令：

| 指令 | 说明 |
|---|---|
| `/rss_sub` | 订阅当前群的 RSS 推送 |
| `/rss_unsub` | 取消订阅当前群的 RSS 推送 |
| `/rss_status` | 查看推送状态（源地址、间隔、已推送条数、推送目标数） |
| `/rss_stop` | 跳过当前所有未推送的帖子，之后的新帖子仍正常推送 |
| `/rss_reset` | 重置已发送记录，下次轮询会重新推送所有条目 |
| `/rss_debug_origin` | 查看当前会话的 `unified_msg_origin`，用于确认 `platform` 配置 |
