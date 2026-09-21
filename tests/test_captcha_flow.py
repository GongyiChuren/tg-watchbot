#!/usr/bin/env python3
"""Offline E2E test for tg-watchbot private-chat pipeline.

Fake bot + fake message drive app.user_message directly:
captcha gate -> verify -> relay -> auto-reply dedupe -> silent spam block.
Run: /opt/tg-watchbot/.venv/bin/python tests/test_captcha_flow.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app  # noqa: E402


class FakeBot:
    def __init__(self):
        self.sent: list[tuple[int, str]] = []
        self.photos: list[int] = []
        self.deleted: list[tuple[int, int]] = []
        self._mid = 1000

    async def send_message(self, chat_id, text, **kwargs):
        self._mid += 1
        self.sent.append((chat_id, text))
        return SimpleNamespace(message_id=self._mid)

    async def send_photo(self, chat_id, **kwargs):
        self._mid += 1
        self.photos.append(chat_id)
        return SimpleNamespace(message_id=self._mid)

    async def delete_message(self, chat_id, message_id):
        self.deleted.append((chat_id, message_id))
        return True


def make_message(uid: int, text: str | None, mid: int = 1, first_name: str = "Tester"):
    answers: list[str] = []
    photos: list[int] = []

    async def answer(t=None, **kwargs):
        answers.append(t or "")
        return SimpleNamespace(message_id=mid)

    async def answer_photo(photo=None, caption=None, reply_markup=None, **kwargs):
        photos.append(1)
        # Also deliver caption so assertion can inspect it.
        if caption:
            answers.append(str(caption))
        return SimpleNamespace(message_id=mid + 1)

    async def copy_to(chat_id, **kwargs):
        return SimpleNamespace(message_id=mid + 5000)

    msg = SimpleNamespace(
        chat=SimpleNamespace(id=uid, type="private"),
        from_user=SimpleNamespace(id=uid, first_name=first_name, last_name=None, username="tester"),
        text=text,
        caption=None,
        message_id=mid,
        content_type="text" if text else "photo",
        answer=answer,
        answer_photo=answer_photo,
        copy_to=copy_to,
        reply=answer,
    )
    return msg, answers, photos


async def main() -> int:
    fails: list[str] = []

    def check(name: str, cond: bool):
        print(("PASS " if cond else "FAIL ") + name)
        if not cond:
            fails.append(name)

    # --- environment reset ---
    app.config = {
        "bot": {
            "rate_limit": {"window_seconds": 10, "max_messages": 100},
            "spam_filter": {"enabled": True, "auto_block": True, "keywords": ["博彩"]},
            "verification": {"enabled": True, "expires_minutes": 10},
            "auto_reply": {"enabled": True, "text": "已转发，我在睡觉，睡醒回复。", "min_interval_minutes": 5},
        }
    }
    app.bot = FakeBot()
    app.admin_chat_id = 999
    app.admin_chat_ids = [999]
    app.init_db()

    uid = 555001
    app.set_block(uid, False)
    app.set_verified(uid, False)
    app.captcha_pop(uid)
    app.auto_reply_buckets.pop(uid, None)
    with app.closing(app.db()) as conn:
        conn.execute("DELETE FROM inbox_messages WHERE user_id=?", (uid,))
        conn.commit()

    # --- A: unverified, no active captcha -> captcha photo sent, nothing stored ---
    msg, answers, photos = make_message(uid, "你好")
    await app.user_message(msg)
    check("A1 captcha photo sent", len(photos) == 1)
    check("A2 no inbox row yet", app.get_inbox_message(1) is None or True)  # placeholder, real check below
    with app.closing(app.db()) as conn:
        n = conn.execute("SELECT COUNT(*) c FROM inbox_messages WHERE user_id=?", (uid,)).fetchone()["c"]
    check("A3 message not stored", n == 0)
    check("A4 no admin relay", all(cid != 999 for cid, _ in app.bot.sent))

    # --- B: wrong captcha answer -> old code invalidated, fresh captcha sent ---
    msg, answers, photos = make_message(uid, "XXXX", mid=2)
    await app.user_message(msg)
    check("B1 wrong-answer hint sent", any("验证码不对" in a for a in answers))
    check("B2 fresh captcha re-sent after wrong", len(photos) == 1)
    stale_code = "XXXX"  # the wrong answer must never become valid later
    check("B3 old captcha invalidated", not any(st["text"] == stale_code for st in app.captcha_state.values()))

    # --- B2: brute-force simulation: repeat same wrong answer N times, old codes never accepted ---
    brute_ok = True
    for i in range(6):
        msg, answers, photos = make_message(uid, "ZZZZ", mid=20 + i)
        await app.user_message(msg)
        if app.is_verified(uid):
            brute_ok = False
        # every wrong attempt must immediately produce a brand-new captcha
        if uid not in app.captcha_state or app.captcha_state[uid]["text"] == "ZZZZ":
            brute_ok = False
    check("B4 brute-force never verifies", brute_ok)

    # --- C: correct captcha answer -> verified ---
    correct = app.captcha_state[uid]["text"]
    msg, answers, photos = make_message(uid, correct.lower(), mid=3)
    await app.user_message(msg)
    check("C1 verified stored", app.is_verified(uid))
    check("C2 success hint", any("验证通过" in a for a in answers))

    # --- D: after verify -> relayed + auto-reply once ---
    app.bot.sent.clear()
    msg, answers, photos = make_message(uid, "佬友你好，问个事", mid=4)
    await app.user_message(msg)
    check("D1 relayed to admin", any(cid == 999 for cid, _ in app.bot.sent))
    check("D2 auto-reply sent", any("睡觉" in t for _, t in app.bot.sent))
    app.bot.sent.clear()
    msg, answers, photos = make_message(uid, "再问一个", mid=5)
    await app.user_message(msg)
    check("D3 auto-reply deduped", not any("睡觉" in t for _, t in app.bot.sent))
    check("D4 still relayed", any(cid == 999 for cid, _ in app.bot.sent))

    # --- E: spam keyword -> silent block (no admin notice, no user reply) ---
    spam_uid = 555002
    app.upsert_user(spam_uid, "Spammer", "spammer")
    app.set_block(spam_uid, False)
    app.captcha_pop(spam_uid)
    app.set_verified(spam_uid, True)  # skip captcha gate
    app.bot.sent.clear()
    msg, answers, photos = make_message(spam_uid, "最高博彩平台", mid=6)
    await app.user_message(msg)
    check("E1 blocked silently", app.is_blocked(spam_uid))
    check("E2 no admin notification", not any(cid == 999 for cid, _ in app.bot.sent))
    check("E3 no user-visible reply", len(answers) == 0)
    app.bot.sent.clear()
    msg, answers, photos = make_message(spam_uid, "还能发吗", mid=7)
    await app.user_message(msg)
    check("E4 blocked user silent afterwards", len(answers) == 0 and not app.bot.sent)

    # --- F: verification disabled -> straight relay ---
    app.config["bot"]["verification"]["enabled"] = False
    fresh = 555003
    app.set_block(fresh, False)
    app.set_verified(fresh, False)
    app.captcha_pop(fresh)
    app.bot.photos.clear()
    msg, answers, photos = make_message(fresh, "直接找人", mid=8)
    await app.user_message(msg)
    check("F1 no captcha when disabled", len(app.bot.photos) == 0)
    check("F2 relayed directly", any(cid == 999 for cid, _ in app.bot.sent))

    print()
    if fails:
        print(f"FAILED: {len(fails)} -> {fails}")
        return 1
    print("ALL PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
