"""Diagnose in-flight RESTORE waits/IO."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import restore_latest_bak as r  # noqa: E402


def main() -> int:
    cfg = r.Config.from_env(r.load_env(r.ENV_FILE))
    queries = {
        "req": (
            "SET NOCOUNT ON; "
            "SELECT "
            "CONVERT(varchar(16), session_id)+'|'+status+'|'+command+'|'+"
            "CONVERT(varchar(32), percent_complete)+'|'+ISNULL(wait_type,N'')+'|'+"
            "CONVERT(varchar(32), wait_time)+'|'+ISNULL(last_wait_type,N'')+'|'+"
            "CONVERT(varchar(32), cpu_time)+'|'+CONVERT(varchar(32), reads)+'|'+"
            "CONVERT(varchar(32), writes)+'|'+CONVERT(varchar(32), logical_reads)+'|'+"
            "CONVERT(varchar(32), DATEDIFF(second,start_time,GETDATE())) "
            "FROM sys.dm_exec_requests WHERE command LIKE N'%RESTORE%';"
        ),
        "waits": (
            "SET NOCOUNT ON; "
            "SELECT TOP 15 "
            "CONVERT(varchar(16), session_id)+'|'+ISNULL(wait_type,N'')+'|'+"
            "CONVERT(varchar(32), wait_duration_ms)+'|'+ "
            "CONVERT(varchar(16), ISNULL(blocking_session_id,0)) "
            "FROM sys.dm_os_waiting_tasks "
            "WHERE wait_type IS NOT NULL "
            "ORDER BY wait_duration_ms DESC;"
        ),
        "dbfiles": (
            "SET NOCOUNT ON; "
            "SELECT "
            "db_name(database_id)+'|'+type_desc+'|'+physical_name+'|'+"
            "CONVERT(varchar(32), size*8/1024) "
            "FROM sys.master_files WHERE db_name(database_id)=N'erp_pm';"
        ),
    }
    for name, q in queries.items():
        print(f"=== {name} ===")
        try:
            print(r.sqlcmd(cfg, q, timeout=30).strip() or "(empty)")
        except Exception as exc:
            print(f"ERR {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
