#!/usr/bin/env python3
"""回归测试：环境变量优先级（.env 不得顶掉 Docker/systemd 注入的变量）。

覆盖 2026-09-27 修复的 bug：compose 里 `WEB_PANEL_HOST=0.0.0.0` 被挂载进容器的
.env（默认 127.0.0.1）反向覆盖，导致面板绑到 loopback、宿主机访问不到。

Run: python tests/test_env_priority.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

FAILS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


# 在子进程里验证，因为 load_dotenv 会污染当前解释器的 os.environ。
_PROBE = '''
import os, sys
from pathlib import Path
sys.path.insert(0, {root!r})
import app
app.load_env()
print("WEB_PANEL_HOST=" + os.getenv("WEB_PANEL_HOST", "<unset>"))
print("WEB_PANEL_PORT=" + os.getenv("WEB_PANEL_PORT", "<unset>"))
print("TELEGRAM_BOT_TOKEN=" + os.getenv("TELEGRAM_BOT_TOKEN", "<unset>"))
'''


def run_probe(env_extra: dict[str, str], env_file: str) -> dict[str, str]:
    """起一个子进程：写一份 .env，再带外部环境变量，看谁生效。"""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        # app.py 的 ENV_PATH 是 BASE_DIR/.env，BASE_DIR 由 __file__ 决定，
        # 所以直接改 .env 副本不方便；这里用 monkeypatch 的方式在子进程里覆盖。
        probe = f'''
import os, sys
from pathlib import Path
sys.path.insert(0, {str(REPO_ROOT)!r})
import app
app.ENV_PATH = Path({str(tmp_path / ".env")!r})
app._EXTERNAL_ENV = {{k: v for k, v in os.environ.items()}}
app.load_env()
for k in ("WEB_PANEL_HOST", "WEB_PANEL_PORT", "TELEGRAM_BOT_TOKEN"):
    print(k + "=" + os.getenv(k, "<unset>"))
'''
        (tmp_path / ".env").write_text(env_file, encoding="utf-8")
        env = dict(os.environ)
        env.update(env_extra)
        env["PYTHONPATH"] = str(REPO_ROOT)
        r = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True, text=True, env=env, cwd=str(REPO_ROOT),
        )
        if r.returncode != 0:
            raise RuntimeError(f"probe failed: {r.stderr[-800:]}")
        out = {}
        for line in r.stdout.strip().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k] = v
        return out


print("=== 场景1：Docker —— compose 注入 0.0.0.0，.env 里是 127.0.0.1 ===")
res = run_probe(
    {"WEB_PANEL_HOST": "0.0.0.0"},
    "WEB_PANEL_HOST=127.0.0.1\nWEB_PANEL_PORT=8765\nTELEGRAM_BOT_TOKEN=from-file\n",
)
check("外部变量 WIN（面板绑 0.0.0.0）", res["WEB_PANEL_HOST"] == "0.0.0.0", res["WEB_PANEL_HOST"])
check(".env 独有键仍能读到", res["WEB_PANEL_PORT"] == "8765", res["WEB_PANEL_PORT"])
check(".env 提供外部没设的键", res["TELEGRAM_BOT_TOKEN"] == "from-file", res["TELEGRAM_BOT_TOKEN"])

print()
print("=== 场景2：纯 systemd/裸跑 —— 无外部注入，.env 生效 ===")
res = run_probe({}, "WEB_PANEL_HOST=127.0.0.1\nWEB_PANEL_PORT=9999\n")
check(".env 值生效（host）", res["WEB_PANEL_HOST"] == "127.0.0.1", res["WEB_PANEL_HOST"])
check(".env 值生效（port）", res["WEB_PANEL_PORT"] == "9999", res["WEB_PANEL_PORT"])

print()
print("=== 场景3：环境变量存在但 .env 无此项 ===")
res = run_probe({"WEB_PANEL_HOST": "0.0.0.0"}, "WEB_PANEL_PORT=8765\n")
check("外部变量保持", res["WEB_PANEL_HOST"] == "0.0.0.0", res["WEB_PANEL_HOST"])

print()
if FAILS:
    print(f"FAILED: {len(FAILS)} -> {FAILS}")
    raise SystemExit(1)
print("ALL PASS")
