"""Poll SQL Server until the configured database becomes ONLINE."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import restore_latest_bak as r  # noqa: E402


def main() -> int:
    cfg = r.Config.from_env(r.load_env(r.ENV_FILE))
    r.setup_logging(cfg.log_dir)
    try:
        r.poll_until_online(cfg)
    except TimeoutError:
        print("POLL_TIMEOUT", flush=True)
        return 1
    except RuntimeError as exc:
        print(f"UNEXPECTED_STATE: {exc}", flush=True)
        return 2
    print("DATABASE_ONLINE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
