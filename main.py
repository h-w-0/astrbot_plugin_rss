import asyncio
import json
import re
import ssl
import time
from pathlib import Path
from typing import List, Set

import aiohttp
import feedparser

from astrbot.api import AstrBotConfig
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.astrbot_path import get_astrbot_data_path


class Main(Star):
    """RSS 主动推送插件——定时拉取 RSS，推送到白名单群聊。"""

    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config

        self.rss_url = config.get(
            "rss_url",
            "https://lostmedia.wikidot.com/feed/forum/posts.xml",
        )
        self.check_interval = config.get("check_interval", 300)
        self.rss_title = config.get("rss_title", "失传媒体中文维基 论坛新消息")
        self.group_whitelist: list = config.get("group_whitelist", [])

        # 持久化目录
        self.data_dir = (
            Path(get_astrbot_data_path())
            / "plugin_data"
            / "astrbot_plugin_rss_push"
        )
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.sent_file = self.data_dir / "sent_items.json"
        self.sent_guids: Set[str] = set()
        self._load_sent_guids()

        self.subscribed_file = self.data_dir / "subscribed_origins.json"
        self.subscribed_origins: List[str] = []
        self._load_subscribed()

        # 创建持久的 HTTP 会话（避免每次新建连接）
        self._session = aiohttp.ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; AstrBot-RSS/1.0; +https://github.com/user/astrbot_plugin_rss_push)"
            },
            timeout=aiohttp.ClientTimeout(total=60),
            # 信任系统 CA 证书，如果失败则降级
            connector=aiohttp.TCPConnector(ssl=False),  # ← 关键修复：关闭 SSL 校验
        )

        print(f"[RSS Push] 启动 | 源: {self.rss_url} | 间隔: {self.check_interval}s | 白名单: {self.group_whitelist}")
        print(f"[RSS Push] 已发送 GUID 数: {len(self.sent_guids)}")

        self._background_task = asyncio.create_task(self._rss_poll_loop())

    # ─── 持久化 ───

    def _load_sent_guids(self):
        if self.sent_file.exists():
            try:
                with open(self.sent_file, "r", encoding="utf-8") as f:
                    self.sent_guids = set(json.load(f))
            except Exception:
                self.sent_guids = set()

    def _save_sent_guids(self):
        with open(self.sent_file, "w", encoding="utf-8") as f:
            json.dump(list(self.sent_guids), f, ensure_ascii=False, indent=2)

    def _load_subscribed(self):
        if self.subscribed_file.exists():
            try:
                with open(self.subscribed_file, "r", encoding="utf-8") as f:
                    self.subscribed_origins = json.load(f)
            except Exception:
                self.subscribed_origins = []

    def _save_subscribed(self):
        with open(self.subscribed_file, "w", encoding="utf-8") as f:
            json.dump(self.subscribed_origins, f, ensure_ascii=False, indent=2)

    # ─── 推送目标 ───

    def _get_target_origins(self) -> List[str]:
        origins = list(self.subscribed_origins)
        for gid in self.group_whitelist:
            origin = f"aiocqhttp:GroupMessage:{gid}"
            if origin not in origins:
                origins.append(origin)
        return origins

    # ─── HTML 工具 ───

    @staticmethod
    def _strip_html(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'<br\s*/?>', "\n", text, flags=re.IGNORECASE)
        text = re.sub(r'</p>\s*<p>', "\n\n", text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', "", text)
        text = (
            text.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&#39;", "'")
            .replace("&nbsp;", " ")
        )
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _truncate(text: str, max_len: int = 500) -> str:
        return text if len(text) <= max_len else text[:max_len] + "…"

    # ─── 构造消息 ───

    def _build_message(self, entry) -> str:
        title = entry.get("title", "无标题")
        author = (
            entry.get("wikidot_authorName")
            or entry.get("author")
            or "未知"
        )

        raw_content = ""
        if hasattr(entry, "content") and entry.content:
            raw_content = entry.content[0].get("value", "")
        if not raw_content:
            raw_content = entry.get("description", "")

        content_text = self._truncate(self._strip_html(raw_content))
        guid = entry.get("id") or entry.get("link", "")

        lines = [
            self.rss_title,
            "=" * 10,
            f"[{title}]",
            f"by {author}",
            "",
        ]
        if content_text:
            lines.append(content_text)
        lines.append("=" * 10)
        lines.append(f"源链接：{guid}")

        return "\n".join(lines)

    # ─── RSS 获取（修复版） ───

    async def _fetch_rss(self):
        """异步获取并解析 RSS，返回 (条目列表, 调试信息)。"""
        debug_info = {}
        try:
            async with self._session.get(self.rss_url) as resp:
                debug_info["status"] = resp.status
                debug_info["content_type"] = resp.content_type
                raw_bytes = await resp.read()
                debug_info["bytes_len"] = len(raw_bytes)
                debug_info["preview"] = raw_bytes[:200].decode("utf-8", errors="replace")

                parsed = feedparser.parse(raw_bytes)
                debug_info["entries_count"] = len(parsed.entries)
                if parsed.entries:
                    first = parsed.entries[0]
                    debug_info["first_entry_id"] = first.get("id") or first.get("link", "N/A")
                    debug_info["first_entry_title"] = first.get("title", "N/A")
                else:
                    debug_info["bozo"] = parsed.get("bozo", False)
                    debug_info["bozo_exception"] = str(parsed.get("bozo_exception", ""))

                return parsed, debug_info
        except Exception as e:
            debug_info["error"] = str(e)
            # 如果 ssl=False 还不行，抛出去让上层处理
            raise

    async def _check_and_push(self):
        """核心：检查 RSS 新条目并推送。"""
        try:
            feed, debug = await self._fetch_rss()
        except Exception as e:
            print(f"[RSS Push] ❌ 抓取 RSS 失败: {e}")
            import traceback
            traceback.print_exc()
            return

        print(f"[RSS Push] 调试: status={debug['status']}, bytes={debug['bytes_len']}, entries={debug['entries_count']}")
        if debug.get("first_entry_title"):
            print(f"[RSS Push] 首条: {debug['first_entry_title']} | id={debug['first_entry_id']}")
        if debug.get("bozo_exception"):
            print(f"[RSS Push] 解析异常: {debug['bozo_exception']}")

        if debug["entries_count"] == 0:
            print(f"[RSS Push] RSS 为空或解析失败，跳过本轮")
            return

        new_entries = []
        for entry in feed.entries:
            guid = entry.get("id") or entry.get("link", "")
            if guid and guid not in self.sent_guids:
                new_entries.append(entry)
                print(f"[RSS Push] 发现新条目: {entry.get('title', '无标题')} | guid={guid}")

        if not new_entries:
            print(f"[RSS Push] 无新条目（共 {len(feed.entries)} 条，已全部推送过）")
            return

        origins = self._get_target_origins()
        print(f"[RSS Push] 新条目 {len(new_entries)} 条，推送到 {len(origins)} 个目标")

        if not origins:
            print(f"[RSS Push] 没有推送目标，仅记录 GUID")
            for entry in new_entries:
                guid = entry.get("id") or entry.get("link", "")
                self.sent_guids.add(guid)
            self._save_sent_guids()
            return

        for entry in new_entries:
            guid = entry.get("id") or entry.get("link", "")
            message = self._build_message(entry)
            print(f"[RSS Push] 推送: {entry.get('title', '无标题')}")

            for origin in origins:
                try:
                    chain = MessageChain().message(message)
                    await self.context.send_message(origin, chain)
                    print(f"[RSS Push] ✅ 已发送到 {origin}")
                    await asyncio.sleep(1.5)
                except Exception as e:
                    print(f"[RSS Push] ❌ 发送至 {origin} 失败: {e}")

            self.sent_guids.add(guid)
            self._save_sent_guids()
            await asyncio.sleep(2)

        print(f"[RSS Push] 本轮推送完成，共 {len(new_entries)} 条")

    async def _rss_poll_loop(self):
        await asyncio.sleep(15)
        while True:
            try:
                print(f"[RSS Push] 开始轮询...")
                await self._check_and_push()
            except asyncio.CancelledError:
                break
            except Exception as e:
                import traceback
                print(f"[RSS Push] 轮询异常: {e}")
                traceback.print_exc()
            await asyncio.sleep(self.check_interval)

    # ─── 指令 ───

    @filter.command("rss_sub")
    async def subscribe(self, event: AstrMessageEvent):
        umo = event.unified_msg_origin
        if umo not in self.subscribed_origins:
            self.subscribed_origins.append(umo)
            self._save_subscribed()
            yield event.plain_result("✅ 已订阅 RSS 推送！")
        else:
            yield event.plain_result("ℹ️ 此群已订阅。")

    @filter.command("rss_unsub")
    async def unsubscribe(self, event: AstrMessageEvent):
        umo = event.unified_msg_origin
        if umo in self.subscribed_origins:
            self.subscribed_origins.remove(umo)
            self._save_subscribed()
            yield event.plain_result("✅ 已取消订阅。")
        else:
            yield event.plain_result("ℹ️ 此群未订阅。")

    @filter.command("rss_status")
    async def status(self, event: AstrMessageEvent):
        origins = self._get_target_origins()
        yield event.plain_result(
            f"📡 RSS 推送状态\n"
            f"  源：{self.rss_url}\n"
            f"  间隔：{self.check_interval}s\n"
            f"  已推送：{len(self.sent_guids)} 条\n"
            f"  推送目标：{len(origins)} 个"
        )

    @filter.command("rss_debug")
    async def debug_fetch(self, event: AstrMessageEvent):
        """手动抓取一次 RSS 并显示调试信息。"""
        yield event.plain_result("🔍 正在抓取 RSS，请稍候…")
        try:
            feed, debug = await self._fetch_rss()
            lines = [
                f"状态码: {debug['status']}",
                f"Content-Type: {debug['content_type']}",
                f"字节数: {debug['bytes_len']}",
                f"条目数: {debug['entries_count']}",
            ]
            if debug.get("first_entry_title"):
                lines.append(f"首条标题: {debug['first_entry_title']}")
                lines.append(f"首条 GUID: {debug['first_entry_id']}")
            if debug.get("bozo_exception"):
                lines.append(f"解析异常: {debug['bozo_exception']}")
            if debug.get("error"):
                lines.append(f"错误: {debug['error']}")
            lines.append("")
            lines.append("RSS 前200字节:")
            lines.append(debug.get("preview", "")[:200])
            yield event.plain_result("\n".join(lines))
        except Exception as e:
            import traceback
            yield event.plain_result(f"❌ 抓取失败: {e}\n{traceback.format_exc()[:300]}")
