# astrbot-plugin-rss

AstrBot 插件 ，可转发Wikidot论坛帖子到QQ群

> 在插件配置页面中，`group_whitelist` 项填入群号列表，如 `["12345678", "87654321"]`。

## 指令

| 指令          | 说明                 |
|---------------|----------------------|
| `/rss_sub`    | 当前群订阅 RSS 推送  |
| `/rss_unsub`  | 当前群取消订阅       |
| `/rss_status` | 查看推送状态         |

## 注意事项

- 插件使用 `aiocqhttp:GroupMessage:{群号}` 格式构造推送目标，若机器人不是基于 OneBot (aiocqhttp) 协议，需自行修改 `_get_target_origins()` 中的平台前缀。
- 已推送过的条目 GUID 会保存在 `data/plugin_data/astrbot_plugin_rss_push/sent_items.json` 中，不会重复推送。
- 检查间隔默认 300 秒（5 分钟），可在 WebUI 中修改。
- 每条消息之间间隔 1.5 秒，防止被 QQ 风控。
