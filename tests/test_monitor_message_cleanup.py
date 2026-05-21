import asyncio
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import ModuleType, SimpleNamespace


def install_import_stubs() -> None:
    class DummyRouter:
        def message(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    def identity_factory(*args, **kwargs):
        return object()

    modules = {
        "feedparser": ModuleType("feedparser"),
        "httpx": ModuleType("httpx"),
        "yaml": ModuleType("yaml"),
        "uvicorn": ModuleType("uvicorn"),
        "apscheduler": ModuleType("apscheduler"),
        "apscheduler.schedulers": ModuleType("apscheduler.schedulers"),
        "apscheduler.schedulers.asyncio": ModuleType("apscheduler.schedulers.asyncio"),
        "bs4": ModuleType("bs4"),
        "dotenv": ModuleType("dotenv"),
        "aiogram": ModuleType("aiogram"),
        "aiogram.enums": ModuleType("aiogram.enums"),
        "aiogram.exceptions": ModuleType("aiogram.exceptions"),
        "aiogram.filters": ModuleType("aiogram.filters"),
        "aiogram.types": ModuleType("aiogram.types"),
        "aiogram.client": ModuleType("aiogram.client"),
        "aiogram.client.default": ModuleType("aiogram.client.default"),
        "fastapi": ModuleType("fastapi"),
        "fastapi.responses": ModuleType("fastapi.responses"),
    }
    modules["apscheduler.schedulers.asyncio"].AsyncIOScheduler = object
    modules["bs4"].BeautifulSoup = object
    modules["dotenv"].load_dotenv = lambda *args, **kwargs: None
    modules["yaml"].safe_load = lambda stream: {"bot": {"spam_filter": {"enabled": True, "keywords": []}}}
    modules["yaml"].safe_dump = lambda data, **kwargs: str(data)
    modules["aiogram"].Bot = object
    modules["aiogram"].Dispatcher = object
    modules["aiogram"].F = object()
    modules["aiogram"].Router = DummyRouter
    modules["aiogram.enums"].ParseMode = SimpleNamespace(HTML="HTML")
    modules["aiogram.exceptions"].TelegramAPIError = Exception
    modules["aiogram.filters"].Command = identity_factory
    modules["aiogram.filters"].CommandObject = object
    modules["aiogram.types"].Message = object
    modules["aiogram.client.default"].DefaultBotProperties = identity_factory
    modules["fastapi"].Depends = identity_factory
    modules["fastapi"].FastAPI = object
    modules["fastapi"].Form = identity_factory
    modules["fastapi"].HTTPException = Exception
    modules["fastapi"].Request = object
    modules["fastapi"].Response = object
    modules["fastapi"].status = object()
    modules["fastapi.responses"].HTMLResponse = object
    modules["fastapi.responses"].RedirectResponse = object
    modules["fastapi.responses"].PlainTextResponse = object
    modules["uvicorn"].Server = object
    modules["uvicorn"].Config = identity_factory
    sys.modules.update({name: sys.modules.get(name, module) for name, module in modules.items()})


install_import_stubs()
import app


class FakeBot:
    def __init__(self) -> None:
        self.deleted: list[tuple[int, int]] = []
        self.sent_texts: list[str] = []
        self.sent_chat_ids: list[int] = []
        self.fail_chat_ids: set[int] = set()

    async def delete_message(self, chat_id: int, message_id: int) -> None:
        self.deleted.append((chat_id, message_id))

    async def send_message(self, chat_id: int, text: str, disable_web_page_preview: bool = False):
        if chat_id in self.fail_chat_ids:
            raise RuntimeError("send failed")
        self.sent_chat_ids.append(chat_id)
        self.sent_texts.append(text)
        return SimpleNamespace(message_id=3003)


class MonitorMessageCleanupTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.old_db_path = app.DB_PATH
        app.DB_PATH = Path(self.temp_dir.name) / "test.sqlite3"
        app.init_db()

    def tearDown(self) -> None:
        app.DB_PATH = self.old_db_path
        self.temp_dir.cleanup()

    def test_monitor_notification_send_is_recorded_for_later_deletion(self) -> None:
        old_bot = app.bot
        old_admin_chat_id = app.admin_chat_id
        old_admin_chat_ids = app.admin_chat_ids
        old_config = app.config
        fake_bot = FakeBot()
        app.bot = fake_bot
        app.admin_chat_id = 1001
        app.admin_chat_ids = []
        app.config = {"cleanup": {"monitor_message_delete_after_minutes": 1}}
        try:
            sent = asyncio.run(app.admin_send_monitor("monitor hit", "NodeSeek 新帖"))
            self.assertTrue(sent)
            self.assertEqual(["monitor hit"], fake_bot.sent_texts)
            with closing(sqlite3.connect(app.DB_PATH)) as conn:
                row = conn.execute(
                    "SELECT chat_id, message_id, monitor_name, delete_after_seconds FROM monitor_messages"
                ).fetchone()
            self.assertEqual((1001, 3003, "NodeSeek 新帖", 60), row)
        finally:
            app.bot = old_bot
            app.admin_chat_id = old_admin_chat_id
            app.admin_chat_ids = old_admin_chat_ids
            app.config = old_config

    def test_monitor_event_history_is_recorded(self) -> None:
        app.record_monitor_event("NodeSeek 新帖", "title", "https://example.com", ["关键词"], False)
        with closing(sqlite3.connect(app.DB_PATH)) as conn:
            row = conn.execute("SELECT monitor_name, title, pushed FROM monitor_events").fetchone()
        self.assertEqual(("NodeSeek 新帖", "title", 0), row)

    def test_monitor_notification_is_sent_to_all_admins(self) -> None:
        old_bot = app.bot
        old_admin_chat_ids = app.admin_chat_ids
        old_config = app.config
        fake_bot = FakeBot()
        app.bot = fake_bot
        app.admin_chat_ids = [1001, 1002, 1003]
        app.config = {"cleanup": {"monitor_message_delete_after_minutes": 1}}
        try:
            self.assertTrue(asyncio.run(app.admin_send_monitor("monitor hit", "NodeSeek 新帖")))
            self.assertEqual([1001, 1002, 1003], fake_bot.sent_chat_ids)
        finally:
            app.bot = old_bot
            app.admin_chat_ids = old_admin_chat_ids
            app.config = old_config

    def test_monitor_notification_continues_when_one_admin_fails(self) -> None:
        old_bot = app.bot
        old_admin_chat_ids = app.admin_chat_ids
        old_config = app.config
        fake_bot = FakeBot()
        fake_bot.fail_chat_ids.add(1002)
        app.bot = fake_bot
        app.admin_chat_ids = [1001, 1002, 1003]
        app.config = {"cleanup": {"monitor_message_delete_after_minutes": 1}}
        try:
            with self.assertLogs("tg-watchbot", level="ERROR"):
                self.assertTrue(asyncio.run(app.admin_send_monitor("monitor hit", "NodeSeek 新帖")))
            self.assertEqual([1001, 1003], fake_bot.sent_chat_ids)
        finally:
            app.bot = old_bot
            app.admin_chat_ids = old_admin_chat_ids
            app.config = old_config

    def test_outbound_message_is_recorded_in_conversation_log(self) -> None:
        app.upsert_user(2001, "User", "user")
        outbox_id = app.create_outbox_message(2001, "reply text", "web:inbox", 4004)
        with closing(sqlite3.connect(app.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT direction, source, text, forwarded FROM inbox_messages WHERE id=?", (outbox_id,)).fetchone()
        self.assertEqual(("out", "web:inbox", "reply text", 1), (row["direction"], row["source"], row["text"], row["forwarded"]))

    def test_save_message_map_supports_message_id_only_payload(self) -> None:
        app.save_message_map(1001, 3003, 2001, 4004)
        with closing(sqlite3.connect(app.DB_PATH)) as conn:
            row = conn.execute(
                "SELECT admin_chat_id, admin_message_id, user_id, user_message_id FROM message_map"
            ).fetchone()
        self.assertEqual((1001, 3003, 2001, 4004), row)

    def test_expired_monitor_message_is_deleted_and_removed_from_queue(self) -> None:
        app.record_monitor_message(1001, 2002, "NodeSeek 新帖", delete_after_seconds=60, sent_at_ts=1000)

        fake_bot = FakeBot()
        deleted_count = asyncio.run(app.delete_expired_monitor_messages(fake_bot, now_ts=1061))

        self.assertEqual(1, deleted_count)
        self.assertEqual([(1001, 2002)], fake_bot.deleted)
        with closing(sqlite3.connect(app.DB_PATH)) as conn:
            remaining = conn.execute("SELECT COUNT(*) FROM monitor_messages").fetchone()[0]
        self.assertEqual(0, remaining)

    def test_unexpired_monitor_message_is_kept(self) -> None:
        app.record_monitor_message(1001, 2002, "NodeSeek 新帖", delete_after_seconds=60, sent_at_ts=1000)

        fake_bot = FakeBot()
        deleted_count = asyncio.run(app.delete_expired_monitor_messages(fake_bot, now_ts=1059))

        self.assertEqual(0, deleted_count)
        self.assertEqual([], fake_bot.deleted)
        with closing(sqlite3.connect(app.DB_PATH)) as conn:
            remaining = conn.execute("SELECT COUNT(*) FROM monitor_messages").fetchone()[0]
        self.assertEqual(1, remaining)


class BotConfigurationTest(unittest.TestCase):
    def test_parse_admin_chat_ids_keeps_unique_first_three(self) -> None:
        self.assertEqual([1, 2, 3], app.parse_admin_chat_ids("1,2 2;3,4"))

    def test_bot_is_not_configured_without_token_or_admin_chat_id(self) -> None:
        old_token = os.environ.pop("TELEGRAM_BOT_TOKEN", None)
        old_admin = os.environ.pop("ADMIN_CHAT_ID", None)
        try:
            self.assertFalse(app.bot_env_configured())
        finally:
            if old_token is not None:
                os.environ["TELEGRAM_BOT_TOKEN"] = old_token
            if old_admin is not None:
                os.environ["ADMIN_CHAT_ID"] = old_admin

    def test_bot_is_configured_with_token_and_admin_chat_id(self) -> None:
        old_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        old_admin = os.environ.get("ADMIN_CHAT_ID")
        os.environ["TELEGRAM_BOT_TOKEN"] = "123456:test-token"
        os.environ["ADMIN_CHAT_ID"] = "1001"
        try:
            self.assertTrue(app.bot_env_configured())
        finally:
            if old_token is None:
                os.environ.pop("TELEGRAM_BOT_TOKEN", None)
            else:
                os.environ["TELEGRAM_BOT_TOKEN"] = old_token
            if old_admin is None:
                os.environ.pop("ADMIN_CHAT_ID", None)
            else:
                os.environ["ADMIN_CHAT_ID"] = old_admin

    def test_write_env_values_preserves_existing_session_secret(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            old_env_path = app.ENV_PATH
            app.ENV_PATH = Path(temp_dir) / ".env"
            app.ENV_PATH.write_text("WEB_PANEL_SESSION_SECRET=keep-me\n", encoding="utf-8")
            try:
                app.write_env_values({
                    "TELEGRAM_BOT_TOKEN": "123456:test-token",
                    "ADMIN_CHAT_ID": "1001",
                    "WEB_PANEL_USER": "admin",
                    "WEB_PANEL_PASSWORD": "change-me",
                })
                self.assertIn(
                    "WEB_PANEL_SESSION_SECRET=keep-me",
                    app.ENV_PATH.read_text(encoding="utf-8"),
                )
            finally:
                app.ENV_PATH = old_env_path


class PanelHtmlContractTest(unittest.TestCase):
    def test_login_form_keeps_expected_fields(self) -> None:
        html = app.login_page()
        self.assertIn("action=/login", html)
        self.assertIn("name=username", html)
        self.assertIn("name=password", html)

    def test_monitor_form_keeps_backend_field_names(self) -> None:
        html = app.monitor_form_html()
        for expected in [
            "action='/monitor/create'",
            "name=name",
            "name=mtype",
            "name=url",
            "name=interval_seconds",
            "name=keywords",
            "name=item_selector",
            "name=title_selector",
            "name=link_selector",
            "name=keyword_match",
            "name=new_item",
            "name=price_change",
            "name=stock_change",
            "name=notify_telegram",
        ]:
            self.assertIn(expected, html)

    def test_monitor_form_can_disable_telegram_notification(self) -> None:
        monitor = {
            "type": "rss",
            "interval_seconds": 60,
            "notify_telegram": False,
            "notify_on": {"keyword_match": True},
        }
        html = app.monitor_form_html(monitor)
        self.assertIn("name=notify_telegram", html)
        self.assertNotIn("name=notify_telegram checked", html)

    def test_layout_groups_navigation_by_domain(self) -> None:
        html = app.layout("测试", "<p>ok</p>")
        for expected in ["<b>消息</b>", "<b>监控</b>", "<b>配置</b>", "<b>系统</b>", "私聊广告拦截", "TG 群监听"]:
            self.assertIn(expected, html)

    def test_inbox_copy_describes_two_way_conversation(self) -> None:
        source = Path("app.py").read_text(encoding="utf-8")
        self.assertIn("这里显示双向机器人对话记录", source)
        self.assertIn("管理员 -> 用户", source)

    def test_users_page_keeps_shared_settings_form(self) -> None:
        source = Path("app.py").read_text(encoding="utf-8")
        self.assertIn("action='/users/settings'", source)
        self.assertIn("这里和“Bot / 面板设置”共用同一份 .env", source)

    def test_group_monitor_form_keeps_backend_field_names(self) -> None:
        html = app.group_monitor_form_html()
        for expected in [
            "action='/group-monitors/create'",
            "name=name",
            "name=chat_id",
            "name=keywords",
            "name=exclude_keywords",
            "name=enabled",
            "name=notify_telegram",
        ]:
            self.assertIn(expected, html)


class SpamAndTemplateConfigTest(unittest.TestCase):
    def test_spam_keyword_hits_follow_config(self) -> None:
        old_config = app.config
        app.config = {"bot": {"spam_filter": {"enabled": True, "keywords": ["博彩", "投资"]}}}
        try:
            self.assertEqual(["博彩"], app.spam_keyword_hits("这里有博彩广告"))
        finally:
            app.config = old_config

    def test_quick_replies_are_loaded_from_config(self) -> None:
        old_config = app.config
        app.config = {"bot": {"quick_replies": [{"title": "收到", "text": "稍后处理"}]}}
        try:
            self.assertEqual("收到", app.list_quick_replies()[0]["title"])
        finally:
            app.config = old_config

    def test_update_spam_keywords_writes_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            old_config_path = app.CONFIG_PATH
            old_config = app.config
            app.CONFIG_PATH = Path(temp_dir) / "config.yaml"
            app.CONFIG_PATH.write_text("bot:\n  spam_filter:\n    enabled: true\n    keywords: []\n", encoding="utf-8")
            app.config = {"bot": {"spam_filter": {"enabled": True, "keywords": []}}}
            try:
                self.assertEqual(["广告"], app.update_spam_keywords("add", "广告"))
                self.assertEqual([], app.update_spam_keywords("delete", "广告"))
            finally:
                app.CONFIG_PATH = old_config_path
                app.config = old_config


class GroupMonitorTest(unittest.TestCase):
    def test_group_monitor_for_chat_returns_enabled_target(self) -> None:
        old_config = app.config
        app.config = {
            "group_monitors": [
                {"enabled": True, "chat_id": -10001, "keywords": ["vps"], "exclude_keywords": []},
                {"enabled": False, "chat_id": -10002, "keywords": ["api"]},
            ]
        }
        try:
            monitor = app.group_monitor_for_chat(-10001)
            self.assertIsNotNone(monitor)
            self.assertEqual(-10001, monitor["chat_id"])
            self.assertIsNone(app.group_monitor_for_chat(-10002))
        finally:
            app.config = old_config

    def test_handle_group_keyword_message_sends_summary_to_admin(self) -> None:
        old_config = app.config
        old_bot = app.bot
        old_admin_chat_ids = app.admin_chat_ids
        fake_bot = FakeBot()
        app.bot = fake_bot
        app.admin_chat_ids = [9001]
        app.config = {
            "group_monitors": [
                {
                    "enabled": True,
                    "name": "测试群",
                    "chat_id": -100100100,
                    "keywords": ["vps", "优惠"],
                    "exclude_keywords": ["求带"],
                    "notify_telegram": True,
                }
            ]
        }
        msg = SimpleNamespace(
            chat=SimpleNamespace(id=-100100100, username="groupdemo", title="测试群"),
            from_user=SimpleNamespace(id=123, first_name="Alice", last_name="", username="alice"),
            text="今晚 vps 有优惠",
            caption=None,
            reply_to_message=None,
            message_id=777,
            content_type="text",
        )
        try:
            ok = asyncio.run(app.handle_group_keyword_message(msg))
            self.assertTrue(ok)
            self.assertEqual([9001], fake_bot.sent_chat_ids)
            self.assertIn("[群关键词命中]", fake_bot.sent_texts[0])
            self.assertIn("命中：vps, 优惠", fake_bot.sent_texts[0])
        finally:
            app.config = old_config
            app.bot = old_bot
            app.admin_chat_ids = old_admin_chat_ids

    def test_handle_group_keyword_message_respects_exclude_keywords(self) -> None:
        old_config = app.config
        old_bot = app.bot
        old_admin_chat_ids = app.admin_chat_ids
        fake_bot = FakeBot()
        app.bot = fake_bot
        app.admin_chat_ids = [9001]
        app.config = {
            "group_monitors": [
                {
                    "enabled": True,
                    "chat_id": -100100100,
                    "keywords": ["vps"],
                    "exclude_keywords": ["求带"],
                    "notify_telegram": True,
                }
            ]
        }
        msg = SimpleNamespace(
            chat=SimpleNamespace(id=-100100100, username="groupdemo", title="测试群"),
            from_user=SimpleNamespace(id=123, first_name="Alice", last_name="", username="alice"),
            text="vps 求带",
            caption=None,
            reply_to_message=None,
            message_id=777,
            content_type="text",
        )
        try:
            ok = asyncio.run(app.handle_group_keyword_message(msg))
            self.assertFalse(ok)
            self.assertEqual([], fake_bot.sent_chat_ids)
        finally:
            app.config = old_config
            app.bot = old_bot
            app.admin_chat_ids = old_admin_chat_ids


if __name__ == "__main__":
    unittest.main()
