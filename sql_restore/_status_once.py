"""One-shot restore status (no secrets)."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import restore_latest_bak as r  # noqa: E402


def main() -> int:
    cfg = r.Config.from_env(r.load_env(r.ENV_FILE))
    state = r.database_state_desc(cfg) or "MISSING"
    prog = r.active_restore_progress(cfg)
    print(f"state={state}")
    if prog is None:
        print("progress=none")
    else:
        print(
            f"progress percent={prog.percent_complete} "
            f"wait={prog.wait_type or 'none'} "
            f"eta_s={prog.eta_seconds} command={prog.command}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
