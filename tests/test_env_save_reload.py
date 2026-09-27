#!/usr/bin/env python3
"""回归测试：面板保存 .env 后，值必须立即生效（不能被外部环境快照回滚）。

覆盖 2026-09-27 环境变量优先级修复引入的风险点：systemd 用 EnvironmentFile
把 .env 的值也注入到进程环境，若 write_env_values 不做特殊处理，
面板改完的值会被旧快照顶回去，导致"保存了但没生效"。

Run: python tests/test_env_priority.py 之后就跑这个
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


PROBE = f'''
import os, sys
from pathlib import Path
sys.path.insert(0, {str(REPO_ROOT)!r})
import app

tmp = Path(os.environ["PROBE_TMP"])
env_path = tmp / ".env"
env_path.write_text("WEB_PANEL_USER=admin\\nWEB_PANEL_PASSWORD=old-pass\\n", encoding="utf-8")

app.ENV_PATH = env_path
# 模拟 systemd EnvironmentFile：.env 的旧值已经进到外部环境
app._EXTERNAL_ENV = {{"WEB_PANEL_USER": "admin", "WEB_PANEL_PASSWORD": "old-pass"}}
os.environ["WEB_PANEL_USER"] = "admin"
os.environ["WEB_PANEL_PASSWORD"] = "old-pass"

# 模拟面板提交新密码
app.write_env_values({{"WEB_PANEL_USER": "admin", "WEB_PANEL_PASSWORD": "new-pass"}})

print("PASSWORD=" + os.getenv("WEB_PANEL_PASSWORD", "<unset>"))
print("FILE_HAS_NEW=" + str("WEB_PANEL_PASSWORD=new-pass" in env_path.read_text(encoding="utf-8")))
'''

with tempfile.TemporaryDirectory() as tmp:
    env = dict(os.environ)
    env["PROBE_TMP"] = tmp
    env["PYTHONPATH"] = str(REPO_ROOT)
    r = subprocess.run([sys.executable, "-c", PROBE], capture_output=True, text=True, env=env, cwd=str(REPO_ROOT))
    if r.returncode != 0:
        print(r.stdout)
        print(r.stderr[-1500:])
        raise SystemExit(1)
    out = dict(line.split("=", 1) for line in r.stdout.strip().splitlines() if "=" in line)

print("=== 面板保存后立即生效（systemd EnvironmentFile 场景）===")
check("新密码写入 .env 文件", out.get("FILE_HAS_NEW") == "True", out.get("FILE_HAS_NEW", ""))
check("新密码在进程环境立即生效", out.get("PASSWORD") == "new-pass", out.get("PASSWORD", ""))

print()
if FAILS:
    print(f"FAILED: {len(FAILS)} -> {FAILS}")
    raise SystemExit(1)
print("ALL PASS")
