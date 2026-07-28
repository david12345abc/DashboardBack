"""Finish a restore that already left SQL Server in ONLINE.

Verifies ONLINE (transaction logs stay on C:) and writes state.json.
"""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import restore_latest_bak as r  # noqa: E402


def main() -> int:
    cfg = r.Config.from_env(r.load_env(r.ENV_FILE))
    r.setup_logging(cfg.log_dir)
    r.wait_for_sql(cfg)

    state_out = r.database_state_desc(cfg)
    if state_out != "ONLINE":
        raise RuntimeError(
            f"Database {cfg.database} is not ONLINE yet: {state_out or 'MISSING'}"
        )

    r.connect_share(cfg)
    source = r.latest_backup(cfg.smb_share)
    r.finish_restored_database(cfg, source.name)

    current = r.fingerprint(source)
    current["database"] = cfg.database
    r.save_state(cfg.state_file, current)
    print(f"FINISH_RESTORE_SUCCESS source={source.name}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        r.logging.exception("Finish restore failed")
        raise SystemExit(1)
