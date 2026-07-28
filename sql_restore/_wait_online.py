"""Wait until erp_pm is ONLINE, printing progress along the way."""
from __future__ import annotations

import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import restore_latest_bak as r  # noqa: E402


def main() -> int:
    cfg = r.Config.from_env(r.load_env(r.ENV_FILE))
    deadline = time.time() + 72 * 3600
    last_pct = None
    while time.time() < deadline:
        state = r.sqlcmd(
            cfg,
            "SET NOCOUNT ON; SELECT state_desc FROM sys.databases WHERE name = N'erp_pm';",
            timeout=30,
        ).replace("\r", "").strip()
        prog = r.sqlcmd(
            cfg,
            "SET NOCOUNT ON; SELECT command, percent_complete, "
            "estimated_completion_time/1000, total_elapsed_time/1000, "
            "ISNULL(wait_type, N'-') "
            "FROM sys.dm_exec_requests WHERE command LIKE N'%RESTORE%';",
            timeout=30,
        ).replace("\r", "").strip()
        print(
            time.strftime("%H:%M:%S"),
            "state=",
            state or "MISSING",
            "progress=",
            prog.replace("\n", " | ") if prog else "none",
            flush=True,
        )
        if state == "ONLINE":
            print("DATABASE_ONLINE", flush=True)
            return 0
        # Detect finished restore process without ONLINE (failure)
        if state in {"RESTORING", "RECOVERY_PENDING", "RECOVERING"} or prog:
            time.sleep(60)
            continue
        if state and state not in {"RESTORING", "RECOVERY_PENDING", "RECOVERING"}:
            print("UNEXPECTED_STATE", flush=True)
            return 2
        time.sleep(60)
    print("POLL_TIMEOUT", flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
