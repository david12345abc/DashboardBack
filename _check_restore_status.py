from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
load_dotenv(Path(r"C:\Users\testii\Downloads\dash\DashboardBack")/".env")
from sql_restore.restore_native import Config, load_env, database_state_desc, active_restore_progress, latest_backup, load_state
values = load_env(Path(r"C:\Users\testii\Downloads\dash\DashboardBack")/".env")
cfg = Config.from_env(values)
print("database", cfg.database)
print("bak_source", cfg.bak_source)
print("db_state", database_state_desc(cfg))
print("active_restore", active_restore_progress(cfg))
state = load_state(cfg.state_file)
print("state", state)
latest = latest_backup(Path(cfg.bak_source))
print("latest_bak", latest.name)
print("latest_mtime_local", datetime.fromtimestamp(latest.stat().st_mtime))
print("latest_size_gb", round(latest.stat().st_size/1024/1024/1024, 2))
from sql_connection import SqlConnection
with SqlConnection().connect_ctx() as conn:
    cur = conn.cursor()
    cur.execute("SELECT DB_NAME() AS db, CAST(DATABASEPROPERTYEX(DB_NAME(), 'Status') AS nvarchar(60)) AS status, GETDATE() AS now")
    print("sql", dict(zip([c[0] for c in cur.description], cur.fetchone())))
    cur.execute("SELECT MAX(_Date_Time) FROM dbo._Document907 WHERE _Posted=0x01 AND _Marked=0x00")
    print("max_receipt_sql_dt", cur.fetchone()[0])
