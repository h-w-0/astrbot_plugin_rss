import asyncio
import json
import re
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

        self._session = aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (compatible; AstrBot-RSS/1.0)"},
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(ssl=False),
        )

        print(f"[RSS Push] 启动 | 源: {self.rss_url} | 间隔: {self.check_interval}s")
        print(f"[RSS Push] 白名单: {self.group_whitelist} | 订阅数: {len(self.subscribed_origins)}")
        print(f"[RSS Push] 已推送: {len(self.sent_guids)} 条")

        self._background_task = asyncio.create_task(self._rss_poll_loop())

    # ─── 持久化 ───

    def _load_sent_guids(self):
        if self.sent_file.exists():
            try:
                with open(self.sent_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.sent_guids = set(data)
                    print(f"[RSS Push] 加载已有 GUID: {len(self.sent_guids)} 条")
            except Exception as e:
                print(f"[RSS Push] 加载 GUID 失败: {e}")
                self.sent_guids = set()

    def _save_sent_guids(self):
        with open(self.sent_file, "w", encoding="utf-8") as f:
            json.dump(list(self.sent_guids), f, ensure_ascii=False, indent=2)
        print(f"[RSS Push] 已保存 GUID: {len(self.sent_guids)} 条到文件")

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

    def _detect_platform(self) -> str:
        if self.subscribed_origins:
            return self.subscribed_origins[0].split(":")[0]
        return self.config.get("platform", "default")

    def _get_target_origins(self) -> List[str]:
        origins = list(self.subscribed_origins)
        platform = self._detect_platform()
        for gid in self.group_whitelist:
            origin = f"{platform}:GroupMessage:{gid}"
            if origin not in origins:
                origins.append(origin)
        print(f"[RSS Push] 推送目标: {origins}")
        return origins

    # ─── 工具 ───

    @staticmethod
    def _strip_html(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'<br\s*/?>', "\n", text, flags=re.IGNORECASE)
        text = re.sub(r'</p>\s*<p>', "\n\n", text, flags=re.IGNORECASE)
        text = re.sub(r'</?p[^>]*>', "", text, flags=re.IGNORECASE)
        text = re.sub(r'<a[^>]*>(.*?)</a>', r"\1", text, flags=re.IGNORECASE)
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

    @staticmethod
    def _extract_forum_info(raw_content: str):
        category = ""
        thread = ""
        if not raw_content:
            return category, thread
        m = re.search(r'论坛分类[：:]\s*(.+?)(?:<br|<BR|$)', raw_content)
        if m:
            category = re.sub(r'<[^>]+>', "", m.group(1)).strip()
        m = re.search(r'论坛讨论串[：:]\s*(.+?)(?:<br|<BR|$)', raw_content)
        if m:
            thread = re.sub(r'<[^>]+>', "", m.group(1)).strip()
        return category, thread

    # ─── 获取作者（已修复：小写 authorname）───

    def _get_author(self, entry) -> str:
        """feedparser 把所有命名空间属性转小写，所以是 wikidot_authorname 不是 wikidot_authorName"""
        candidates = [
            lambda: getattr(entry, "wikidot_authorname", None),  # 小写 n
            lambda: entry.get("wikidot_authorname"),
            lambda: getattr(entry, "author", None),
            lambda: entry.get("author"),
            lambda: getattr(entry, "wikidot_authorName", None),  # 兼容大写
            lambda: entry.get("wikidot_authorName"),
        ]
        for fn in candidates:
            try:
                val = fn()
                if val:
                    return str(val)
            except Exception:
                continue
        return "未知"

    # ─── 构造消息 ───

    def _build_message(self, entry) -> str:
        title = getattr(entry, "title", None) or entry.get("title", "无标题")
        author = self._get_author(entry)

        raw_content = ""
        if hasattr(entry, "content") and entry.content:
            raw_content = entry.content[0].get("value", "")
        if not raw_content:
            raw_content = entry.get("description", "")

        category, thread = self._extract_forum_info(raw_content)

        clean_content = raw_content
        clean_content = re.sub(
            r'<br\s*/?>\s*论坛分类[：:].*?(?:<br|<BR|$)',
            "", clean_content, flags=re.IGNORECASE
        )
        clean_content = re.sub(
            r'<br\s*/?>\s*论坛讨论串[：:].*?(?:<br|<BR|$)',
            "", clean_content, flags=re.IGNORECASE
        )

        content_text = self._truncate(self._strip_html(clean_content))
        guid = getattr(entry, "id", None) or getattr(entry, "link", None) or ""

        lines = [
            self.rss_title,
            "=" * 10,
            f"[{title}]",
            f"by {author}",
            "-" * 10,
        ]
        if content_text:
            lines.append(content_text)
        lines.append("=" * 10)
        if category:
            lines.append(f"论坛分类: {category}")
        if thread:
            lines.append(f"论坛讨论串: {thread}")
        lines.append(f"源链接：{guid}")

        return "\n".join(lines)

    # ─── RSS 获取 ───

    async def _fetch_rss(self):
        async with self._session.get(self.rss_url) as resp:
            raw_bytes = await resp.read()
            return feedparser.parse(raw_bytes)

    async def _check_and_push(self):
        feed = await self._fetch_rss()

        if not feed.entries:
            print(f"[RSS Push] RSS 无条目")
            return

        new_entries = []
        for entry in feed.entries:
            guid = getattr(entry, "id", None) or getattr(entry, "link", None) or ""
            if guid and guid not in self.sent_guids:
                new_entries.append((guid, entry))

        if not new_entries:
            print(f"[RSS Push] 无新条目（共 {len(feed.entries)} 条）")
            return

        origins = self._get_target_origins()
        print(f"[RSS Push] {len(new_entries)} 条新 → {len(origins)} 个目标")

        if not origins:
            for guid, _ in new_entries:
                self.sent_guids.add(guid)
            self._save_sent_guids()
            return

        for guid, entry in new_entries:
            message = self._build_message(entry)
        
            for origin in origins:
                try:
                    chain = MessageChain().message(message)
                    await self.context.send_message(origin, chain)
                    print(f"[RSS Push] ✅ 发送至 {origin}")
                    await asyncio.sleep(1.5)
                except Exception as e:
                    print(f"[RSS Push] ❌ {origin}: {e}")
                    import traceback
                    traceback.print_exc()

            # GUID 放到 origins 循环外面，无论如何都标记已发送
            self.sent_guids.add(guid)
            self._save_sent_guids()
            await asyncio.sleep(2)

        print(f"[RSS Push] 本轮完成")

    async def _rss_poll_loop(self):
        await asyncio.sleep(15)
        while True:
            try:
                print(f"[RSS Push] 轮询...")
                await self._check_and_push()
            except asyncio.CancelledError:
                break
            except Exception as e:
                import traceback
                print(f"[RSS Push] 异常: {e}")
                traceback.print_exc()
            await asyncio.sleep(self.check_interval)

    # ─── 指令 ───

    @filter.command("rss_sub")
    async def subscribe(self, event: AstrMessageEvent):
        umo = event.unified_msg_origin
        if umo not in self.subscribed_origins:
            self.subscribed_origins.append(umo)
            self._save_subscribed()
            yield event.plain_result(f"✅ 已订阅 RSS 推送！(origin={umo})")
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

    @filter.command("rss_reset")
    async def reset_sent(self, event: AstrMessageEvent):
        old = len(self.sent_guids)
        self.sent_guids.clear()
        self._save_sent_guids()
        yield event.plain_result(f"🔄 已重置！清除 {old} 条记录。")

    @filter.command("rss_stop")
    async def stop_current(self, event: AstrMessageEvent):
        try:
            feed = await self._fetch_rss()
            count = 0
            for entry in feed.entries:
                guid = getattr(entry, "id", None) or getattr(entry, "link", None) or ""
                if guid and guid not in self.sent_guids:
                    self.sent_guids.add(guid)
                    count += 1
            self._save_sent_guids()
            yield event.plain_result(
                f"⏸️ 已跳过 {count} 条当前未推送的帖子，之后的新帖子仍会正常推送。"
            )
        except Exception as e:
            yield event.plain_result(f"❌ 失败: {e}")

    @filter.command("rss_status")
    async def status(self, event: AstrMessageEvent):
        origins = self._get_target_origins()
        yield event.plain_result(
            f"📡 RSS 推送状态\n"
            f"  源：{self.rss_url}\n"
            f"  间隔：{self.check_interval}s\n"
            f"  已推送：{len(self.sent_guids)} 条\n"
            f"  推送目标：{len(origins)} 个\n"
            f"  已订阅 origins: {json.dumps(self.subscribed_origins)}"
        )

    @filter.command("rss_debug_origin")
    async def debug_origin(self, event: AstrMessageEvent):
        umo = event.unified_msg_origin
        yield event.plain_result(
            f"🔍 当前会话 unified_msg_origin:\n"
            f"  {umo}\n\n"
            f"已订阅 origins: {json.dumps(self.subscribed_origins, ensure_ascii=False)}\n\n"
            f"检测到平台: {self._detect_platform()}\n\n"
            f"构造的目标: {json.dumps(self._get_target_origins(), ensure_ascii=False)}"
        )

    @filter.command("rss_debug_entry")
    async def debug_entry(self, event: AstrMessageEvent):
        try:
            feed = await self._fetch_rss()
            if not feed.entries:
                yield event.plain_result("RSS 中没有任何条目。")
                return
            entry = feed.entries[0]
            fields = []
            for key in sorted(entry.keys()):
                val = entry[key]
                if isinstance(val, (str, int, float, bool)):
                    fields.append(f"  {key} = {val}")
                elif isinstance(val, list):
                    fields.append(f"  {key} = [list: {len(val)} items]")
                elif val is None:
                    fields.append(f"  {key} = None")
                else:
                    fields.append(f"  {key} = {type(val).__name__}: {str(val)[:100]}")
            extra_attrs = ["wikidot_authorname", "author"]
            for attr in extra_attrs:
                val = getattr(entry, attr, None)
                if val is not None:
                    fields.append(f"  attr.{attr} = {val}")
            yield event.plain_result(
                f"🔍 标题: {getattr(entry, 'title', '?')}\n"
                f"作者探测: {self._get_author(entry)}\n"
                f"--- 字段 ---\n" + "\n".join(fields[:30])
            )
        except Exception as e:
            import traceback
            yield event.plain_result(f"❌ 错误: {e}")
