"""Sample restore progress / IO over a short window for ETA."""
from __future__ import annotations

import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import restore_latest_bak as r  # noqa: E402

SAMPLE_SECONDS = 600


def snapshot(cfg: r.Config) -> dict:
    state = r.database_state_desc(cfg) or "MISSING"
    prog = r.active_restore_progress(cfg)
    q = (
        "SET NOCOUNT ON; "
        "SELECT "
        "CONVERT(varchar(32), percent_complete)+'|'+"
        "ISNULL(wait_type,N'')+'|'+"
        "CONVERT(varchar(32), estimated_completion_time)+'|'+"
        "CONVERT(varchar(32), DATEDIFF(second,start_time,GETDATE()))+'|'+"
        "CONVERT(varchar(64), ISNULL(total_elapsed_time,0))+'|'+"
        "ISNULL(command,N'') "
        "FROM sys.dm_exec_requests WHERE command LIKE N'%RESTORE%';"
    )
    raw = r.sqlcmd(cfg, q, timeout=30).strip().replace("\r", "")
    q2 = (
        "SET NOCOUNT ON; "
        "SELECT TOP 5 "
        "ISNULL(wait_type,N'')+'|'+CONVERT(varchar(32), wait_duration_ms)+'|'+ "
        "CONVERT(varchar(32), session_id) "
        "FROM sys.dm_os_waiting_tasks "
        "WHERE wait_type IS NOT NULL AND wait_type NOT LIKE N'SLEEP%' "
        "AND wait_type NOT LIKE N'XE%' "
        "AND wait_type NOT LIKE N'SQLTRACE%' "
        "ORDER BY wait_duration_ms DESC;"
    )
    waits = r.sqlcmd(cfg, q2, timeout=30).strip().replace("\r", "")
    mdf = cfg.data_dir / f"{cfg.database}_data_1.mdf"
    ldf = cfg.log_data_dir / f"{cfg.database}_log_1.ldf"
    return {
        "state": state,
        "percent": prog.percent_complete if prog else None,
        "wait": prog.wait_type if prog else None,
        "eta_s": prog.eta_seconds if prog else None,
        "raw": raw,
        "waits": waits,
        "mdf": mdf.stat().st_size if mdf.exists() else 0,
        "ldf": ldf.stat().st_size if ldf.exists() else 0,
        "c_free": r.free_bytes(cfg.log_data_dir),
        "d_free": r.free_bytes(cfg.data_dir),
        "t": time.time(),
    }


def main() -> int:
    cfg = r.Config.from_env(r.load_env(r.ENV_FILE))
    a = snapshot(cfg)
    print(
        f"T0 state={a['state']} pct={a['percent']} wait={a['wait']} "
        f"eta_s={a['eta_s']} mdfGB={a['mdf']/1024**3:.3f} "
        f"ldfGB={a['ldf']/1024**3:.3f} CfreeGB={a['c_free']/1024**3:.3f} "
        f"DfreeGB={a['d_free']/1024**3:.3f}"
    )
    print(f"T0 raw={a['raw']!r}")
    print(f"T0 waits:\n{a['waits']}")
    print(f"Sampling {SAMPLE_SECONDS}s ...", flush=True)
    time.sleep(SAMPLE_SECONDS)
    b = snapshot(cfg)
    dt = max(b["t"] - a["t"], 0.001)
    c_written = a["c_free"] - b["c_free"]
    d_written = a["d_free"] - b["d_free"]
    ldf_delta = b["ldf"] - a["ldf"]
    mdf_delta = b["mdf"] - a["mdf"]
    pct_delta = 0.0
    if a["percent"] is not None and b["percent"] is not None:
        pct_delta = b["percent"] - a["percent"]
    print(
        f"T1 state={b['state']} pct={b['percent']} wait={b['wait']} "
        f"eta_s={b['eta_s']} mdfGB={b['mdf']/1024**3:.3f} "
        f"ldfGB={b['ldf']/1024**3:.3f} CfreeGB={b['c_free']/1024**3:.3f} "
        f"DfreeGB={b['d_free']/1024**3:.3f}"
    )
    print(f"T1 raw={b['raw']!r}")
    print(f"dt_s={dt:.1f}")
    print(
        f"deltas pct={pct_delta} mdfGB={mdf_delta/1024**3:.3f} "
        f"ldfGB={ldf_delta/1024**3:.3f} CwriteGB={c_written/1024**3:.3f} "
        f"DwriteGB={d_written/1024**3:.3f}"
    )
    print(
        f"rates C_MBps={c_written/1024**2/dt:.2f} "
        f"D_MBps={d_written/1024**2/dt:.2f} "
        f"ldf_MBps={ldf_delta/1024**2/dt:.2f}"
    )

    # ETA heuristics
    log_needed = 173.81 * 1024**3
    bak_bytes = 586.50 * 1024**3
    elapsed_restore_s = None
    if b["raw"] and "|" in b["raw"]:
        parts = b["raw"].split("|")
        if len(parts) >= 4 and parts[3].isdigit():
            elapsed_restore_s = int(parts[3])

    print(f"elapsed_restore_s={elapsed_restore_s}")

    if b["eta_s"] and b["eta_s"] > 0:
        print(f"SQL_ETA_h={b['eta_s']/3600:.2f}")

    if pct_delta > 0.05 and b["percent"] is not None and b["percent"] > 0:
        rem = (100.0 - b["percent"]) * (dt / pct_delta)
        print(f"PCT_ETA_remaining_h={rem/3600:.2f}")
        if elapsed_restore_s:
            total = elapsed_restore_s + rem
            print(f"PCT_ETA_total_h={total/3600:.2f}")

    # If still in FILEOPS creating log: use C write rate
    if (b["wait"] or "").startswith("PREEMPTIVE_OS_FILEOPS") and b["ldf"] < log_needed * 0.99:
        remaining_log = max(log_needed - b["ldf"], 0)
        rate = max(c_written, ldf_delta) / dt
        if rate > 1 * 1024**2:  # >1 MiB/s
            log_eta = remaining_log / rate
            # After log init, restore ~586GB from SMB; assume similar or lower than local write
            # Use conservative 40-80 MB/s SMB range until percent moves
            smb_lo, smb_hi = 40 * 1024**2, 100 * 1024**2
            data_lo = bak_bytes / smb_hi
            data_hi = bak_bytes / smb_lo
            print(f"LOG_INIT_ETA_h={log_eta/3600:.2f} at {rate/1024**2:.1f} MiB/s")
            print(
                f"ROUGH_TOTAL_RANGE_h={(log_eta+data_lo)/3600:.1f}-"
                f"{(log_eta+data_hi)/3600:.1f} "
                f"(log init + bak stream 40-100 MiB/s)"
            )
        else:
            print("LOG_INIT_RATE_TOO_LOW_FOR_ETA")
            # fallback bak-only rough after FILEOPS eventually finishes
            smb_lo, smb_hi = 30 * 1024**2, 80 * 1024**2
            # add uncertain log init: 174GB at 20-80 MB/s
            log_lo = log_needed / (80 * 1024**2)
            log_hi = log_needed / (20 * 1024**2)
            data_lo = bak_bytes / smb_hi
            data_hi = bak_bytes / smb_lo
            print(
                f"FALLBACK_RANGE_h={(log_lo+data_lo)/3600:.1f}-"
                f"{(log_hi+data_hi)/3600:.1f} "
                f"(log zero-fill 20-80 MiB/s + bak 30-80 MiB/s; "
                f"current sample too slow to refine)"
            )

    if b["state"] == "ONLINE" and b["percent"] is None:
        print("ALREADY_ONLINE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
