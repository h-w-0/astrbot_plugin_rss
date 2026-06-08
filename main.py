import asyncio
import json
import re
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

        # ---------- 从配置读取 ----------
        self.rss_url = config.get(
            "rss_url",
            "https://lostmedia.wikidot.com/feed/forum/posts.xml",
        )
        self.check_interval = config.get("check_interval", 300)  # 秒
        self.rss_title = config.get("rss_title", "失传媒体中文维基 论坛新消息")
        self.group_whitelist: list = config.get("group_whitelist", [])

        # ---------- 数据持久化目录 ----------
        self.data_dir = (
            Path(get_astrbot_data_path())
            / "plugin_data"
            / "astrbot_plugin_rss_push"
        )
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # 已发送的条目 GUID（防重复推送）
        self.sent_file = self.data_dir / "sent_items.json"
        self.sent_guids: Set[str] = set()
        self._load_sent_guids()

        # 运行时通过 /rss_sub 命令订阅的会话
        self.subscribed_file = self.data_dir / "subscribed_origins.json"
        self.subscribed_origins: List[str] = []
        self._load_subscribed()

        # ---------- 启动后台轮询任务 ----------
        self._background_task = asyncio.create_task(self._rss_poll_loop())

    # ──────────────────── 持久化读写 ────────────────────

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

    # ──────────────────── 获取所有推送目标 ────────────────────

    def _get_target_origins(self) -> List[str]:
        """合并「配置白名单」+「运行时订阅」的推送目标。"""
        origins = list(self.subscribed_origins)

        for gid in self.group_whitelist:
            origin = f"aiocqhttp:GroupMessage:{gid}"
            if origin not in origins:
                origins.append(origin)

        return origins

    # ──────────────────── HTML 工具 ────────────────────

    @staticmethod
    def _strip_html(text: str) -> str:
        """将 HTML 转为纯文本。"""
        if not text:
            return ""
        # <br> 转成换行
        text = re.sub(r'<br\s*/?>', "\n", text, flags=re.IGNORECASE)
        text = re.sub(r'</p>\s*<p>', "\n\n", text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', "", text)
        # HTML 实体解码
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

    # ──────────────────── 构造推送消息 ────────────────────

    def _build_message(self, entry) -> str:
        title = entry.get("title", "无标题")
        author = (
            entry.get("wikidot_authorName")
            or entry.get("author")
            or "未知"
        )

        # 优先取 content:encoded，其次 description
        raw_content = ""
        if hasattr(entry, "content") and entry.content:
            raw_content = entry.content[0].get("value", "")
        if not raw_content:
            raw_content = entry.get("description", "")

        content_text = self._truncate(self._strip_html(raw_content))

        # GUID / 源链接
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

    # ──────────────────── RSS 获取与推送 ────────────────────

    async def _fetch_rss(self):
        """异步获取并解析 RSS。"""
        async with aiohttp.ClientSession() as session:
            async with session.get(self.rss_url, timeout=30) as resp:
                xml_data = await resp.text()
                return feedparser.parse(xml_data)

    async def _check_and_push(self):
        """核心：检查 RSS 新条目并推送到所有目标。"""
        feed = await self._fetch_rss()

        # 找出未发送过的条目
        new_entries = []
        for entry in feed.entries:
            guid = entry.get("id") or entry.get("link", "")
            if guid and guid not in self.sent_guids:
                new_entries.append(entry)

        if not new_entries:
            return

        # 按时间正序推送（最旧的最先）
        new_entries.reverse()

        origins = self._get_target_origins()
        if not origins:
            # 没有任何推送目标，只记录但不推送
            for entry in new_entries:
                guid = entry.get("id") or entry.get("link", "")
                self.sent_guids.add(guid)
            self._save_sent_guids()
            return

        for entry in new_entries:
            guid = entry.get("id") or entry.get("link", "")
            message = self._build_message(entry)

            for origin in origins:
                try:
                    chain = MessageChain().message(message)
                    await self.context.send_message(origin, chain)
                    await asyncio.sleep(1.5)  # 群消息节流，防止被风控
                except Exception as e:
                    print(f"[RSS Push] 发送至 {origin} 失败: {e}")

            # 标记已推送
            self.sent_guids.add(guid)
            self._save_sent_guids()

            # 条目之间也间隔一下
            await asyncio.sleep(2)

    async def _rss_poll_loop(self):
        """后台轮询循环。"""
        await asyncio.sleep(15)  # 等待插件完全就绪
        while True:
            try:
                await self._check_and_push()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[RSS Push] 轮询异常: {e}")
            await asyncio.sleep(self.check_interval)

    # ──────────────────── 指令处理 ────────────────────

    @filter.command("rss_sub")
    async def subscribe(self, event: AstrMessageEvent):
        """订阅当前会话的 RSS 推送。"""
        umo = event.unified_msg_origin
        if umo not in self.subscribed_origins:
            self.subscribed_origins.append(umo)
            self._save_subscribed()
            yield event.plain_result("✅ 已订阅 RSS 推送！有新内容时会自动推送到此群。")
        else:
            yield event.plain_result("ℹ️ 此群已订阅 RSS 推送。")

    @filter.command("rss_unsub")
    async def unsubscribe(self, event: AstrMessageEvent):
        """取消订阅当前会话的 RSS 推送。"""
        umo = event.unified_msg_origin
        if umo in self.subscribed_origins:
            self.subscribed_origins.remove(umo)
            self._save_subscribed()
            yield event.plain_result("✅ 已取消订阅 RSS 推送。")
        else:
            yield event.plain_result("ℹ️ 此群未订阅 RSS 推送。")

    @filter.command("rss_status")
    async def status(self, event: AstrMessageEvent):
        """查看 RSS 推送状态。"""
        origins = self._get_target_origins()
        yield event.plain_result(
            f"📡 RSS 推送状态\n"
            f"  源：{self.rss_url}\n"
            f"  间隔：{self.check_interval} 秒\n"
            f"  已推送：{len(self.sent_guids)} 条\n"
            f"  推送目标：{len(origins)} 个"
        )
