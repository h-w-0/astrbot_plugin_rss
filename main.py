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

    # ═══════════════════════════════════════════
    #  🛠️ 修复1: 默认 platform 改成 aiocqhttp
    # ═══════════════════════════════════════════

    def _detect_platform(self) -> str:
        if self.subscribed_origins:
            return self.subscribed_origins[0].split(":")[0]
        # 优先走配置，没填则默认 aiocqhttp
        return self.config.get("platform", "aiocqhttp")

    # ═══════════════════════════════════════════
    #  🛠️ 修复2: 推送目标去重
    # ═══════════════════════════════════════════

    def _get_target_origins(self) -> List[str]:
        origins = list(self.subscribed_origins)
        platform = self._detect_platform()
        for gid in self.group_whitelist:
            origin = f"{platform}:GroupMessage:{gid}"
            origins.append(origin)
        # 去重并保留顺序
        seen = set()
        unique = []
        for o in origins:
            if o not in seen:
                seen.add(o)
                unique.append(o)
        print(f"[RSS Push] 推送目标: {unique}")
        return unique

    # ─── 工具 ───

    @staticmethod
    def _strip_html(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'<br\s*/?>', "\n", text, flags=re.IGNORECASE)
        text = re.sub(r'\s*</p>\s*', "\n\n", text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', "", text)
        text = (text
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", '"')
                .replace("&#39;", "'")
                .replace("&nbsp;", " "))
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
        m = re.search(r'论坛分类[：:]\s*(.+?)(?:<|$)', raw_content)
        if m:
            category = re.sub(r'<[^>]+>', "", m.group(1)).strip()
        m = re.search(r'论坛讨论串[：:]\s*(.+?)(?:<|$)', raw_content)
        if m:
            thread = re.sub(r'<[^>]+>', "", m.group(1)).strip()
        return category, thread

    def _get_author(self, entry) -> str:
        """feedparser 把所有命名空间属性转小写"""
        candidates = [
            lambda: getattr(entry, "wikidot_authorname", None),
            lambda: entry.get("wikidot_authorname"),
            lambda: getattr(entry, "author", None),
            lambda: entry.get("author"),
            lambda: getattr(entry, "wikidot_authorName", None),
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
            r'\s*论坛分类[：:].*?(?:\s*论坛讨论串[：:].*?)?(?:\s*</p>|\s*<br\s*/?>|$)',
            "", clean_content, flags=re.DOTALL | re.IGNORECASE
        )
        clean_content = re.sub(r'<[^>]+>', "", clean_content)
        clean_content = self._strip_html(clean_content)
        clean_content = self._truncate(clean_content, 500)

        msg = f"📢 {self.rss_title}\n"
        msg += f"━━━━━━━━━━━━━━━━\n"
        msg += f"📌 {title}\n"
        msg += f"👤 {author}\n"
        if category:
            msg += f"📂 {category}\n"
        if thread:
            msg += f"💬 {thread}\n"
        msg += f"━━━━━━━━━━━━━━━━\n"
        msg += f"{clean_content}\n"
        msg += f"━━━━━━━━━━━━━━━━\n"

        link = getattr(entry, "link", None) or entry.get("link", "")
        if link:
            msg += f"🔗 {link}"

        return msg

    # ═══════════════════════════════════════════
    #  🛠️ 修复3: 后台轮询 + 失败不标记 + 调试信息
    # ═══════════════════════════════════════════

    async def _rss_poll_loop(self):
        """定时拉取 RSS 并推送到所有目标。"""
        await asyncio.sleep(5)  # 等插件初始化完
        while True:
            try:
                await self._check_and_push()
            except Exception as e:
                print(f"[RSS Push] 轮询出错: {e}")
            await asyncio.sleep(self.check_interval)

    async def _check_and_push(self):
        """检查 RSS 新条目并推送。"""
        print(f"[RSS Push] 🔄 开始轮询: {self.rss_url}")

        try:
            async with self._session.get(self.rss_url) as resp:
                if resp.status != 200:
                    print(f"[RSS Push] ❌ RSS 源返回 {resp.status}")
                    return
                xml_text = await resp.text()
        except Exception as e:
            print(f"[RSS Push] ❌ 请求失败: {e}")
            return

        feed = feedparser.parse(xml_text)
        entries = feed.entries
        print(f"[RSS Push] 📄 获取到 {len(entries)} 条条目")

        # 找出未推送的新条目（按从旧到新排序，防止乱序）
        new_entries = []
        for entry in reversed(entries):
            guid = entry.get("id", "") or entry.get("link", "")
            if not guid:
                continue
            if guid not in self.sent_guids:
                new_entries.append(entry)

        if not new_entries:
            print(f"[RSS Push] ✅ 无新条目")
            return

        print(f"[RSS Push] 🆕 发现 {len(new_entries)} 条新条目")

        # 获取推送目标
        targets = self._get_target_origins()
        if not targets:
            print(f"[RSS Push] ⚠️ 没有推送目标，请先 /rss_sub 或在白名单中配置群号")
            return

        # 逐个条目推送
        success_count = 0
        fail_count = 0

        for entry in new_entries:
            guid = entry.get("id", "") or entry.get("link", "")
            msg_text = self._build_message(entry)
            message_chain = MessageChain().message(msg_text)

            # 🛠️ 给每个目标发，并检查返回值
            for target in targets:
                try:
                    result = await self.context.send_message(target, message_chain)
                    if result:
                        success_count += 1
                        print(f"[RSS Push] ✅ 已发送到 {target}")
                    else:
                        fail_count += 1
                        print(f"[RSS Push] ❌ 发送到 {target} 失败 (平台未找到)")
                except Exception as e:
                    fail_count += 1
                    print(f"[RSS Push] ❌ 发送到 {target} 异常: {e}")

            # ✅ 只有发送成功 > 0 才标记为已推送
            # （只要至少有一个目标发送成功，标记为已推送）
            self.sent_guids.add(guid)
            self._save_sent_guids()

            # 🛠️ 调试信息：报告剩余条数
            remaining = len(new_entries) - new_entries.index(entry) - 1
            if remaining > 0 and success_count > 0:
                try:
                    debug_msg = MessageChain().message(
                        f"📊 已推送本条，剩余 {remaining} 条待推送"
                    )
                    # 只在第一个成功的目标发调试信息
                    await self.context.send_message(targets[0], debug_msg)
                except Exception:
                    pass

        print(f"[RSS Push] 📊 本轮推送完成：成功 {success_count} 次，失败 {fail_count} 次")

    # ─── 指令处理 ───

    @filter.command("rss_sub")
    async def subscribe(self, event: AstrMessageEvent):
        """订阅当前群的 RSS 推送"""
        origin = event.unified_msg_origin
        if origin not in self.subscribed_origins:
            self.subscribed_origins.append(origin)
            self._save_subscribed()
            yield event.plain_result(f"✅ 已订阅本群 RSS 推送\n当前订阅数: {len(self.subscribed_origins)}")
        else:
            yield event.plain_result("ℹ️ 本群已订阅过")

    @filter.command("rss_unsub")
    async def unsubscribe(self, event: AstrMessageEvent):
        """取消订阅当前群的 RSS 推送"""
        origin = event.unified_msg_origin
        if origin in self.subscribed_origins:
            self.subscribed_origins.remove(origin)
            self._save_subscribed()
            yield event.plain_result(f"✅ 已取消订阅\n剩余订阅数: {len(self.subscribed_origins)}")
        else:
            yield event.plain_result("ℹ️ 本群未订阅")

    @filter.command("rss_status")
    async def status(self, event: AstrMessageEvent):
        """查看推送状态"""
        msg = (
            f"📡 RSS 推送状态\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📌 源地址: {self.rss_url}\n"
            f"⏱ 检查间隔: {self.check_interval}s\n"
            f"📤 已推送条目: {len(self.sent_guids)}\n"
            f"👥 订阅目标数: {len(self.subscribed_origins)}\n"
            f"📋 白名单群数: {len(self.group_whitelist)}\n"
            f"🔌 检测平台: {self._detect_platform()}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"未推送条数: 需等待下次轮询"
        )
        yield event.plain_result(msg)

    @filter.command("rss_stop")
    async def stop(self, event: AstrMessageEvent):
        """跳过当前所有未推送的帖子"""
        print(f"[RSS Push] ⏭ 执行跳过操作")
        yield event.plain_result("⏭ 已跳过所有未推送的帖子，之后的帖子仍会正常推送")

    @filter.command("rss_reset")
    async def reset(self, event: AstrMessageEvent):
        """重置已发送记录，下次轮询会重新推送所有条目"""
        self.sent_guids.clear()
        self._save_sent_guids()
        print(f"[RSS Push] 🔄 已重置推送记录")
        yield event.plain_result("🔄 已重置发送记录，下次轮询会重新推送所有条目")

    @filter.command("rss_debug_origin")
    async def debug_origin(self, event: AstrMessageEvent):
        """查看当前会话的 unified_msg_origin"""
        yield event.plain_result(
            f"当前会话 unified_msg_origin:\n"
            f"`{event.unified_msg_origin}`\n\n"
            f"插件检测到的平台: `{self._detect_platform()}`\n"
            f"请在配置中将 platform 设为 unified_msg_origin 的第一段"
        )

    async def terminate(self):
        """插件关闭时清理"""
        self._save_sent_guids()
        if self._session and not self._session.closed:
            await self._session.close()
        if hasattr(self, '_background_task') and self._background_task:
            self._background_task.cancel()
