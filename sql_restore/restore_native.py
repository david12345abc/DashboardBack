r"""Native daily restore of the newest erp_pm .bak from Z:\ into local SQL Server.

Compares the latest Z:\*.bak with D:\erp_pm*.bak. If the local copy is the same
file (name + size + mtime), skips. Otherwise: delete local bak, DROP erp_pm,
purge orphan data/log files, copy ~100 GiB bak to D:\, RESTORE with data on
D:\mssql\data and transaction logs on C:\mssql\logdata.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

if os.name == "nt":
    import ctypes
    import msvcrt
    from ctypes import wintypes


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
ENV_FILE = PROJECT_DIR / ".env"
COPY_BUFFER_BYTES = 16 * 1024 * 1024
COPY_LOG_INTERVAL_SECONDS = 30
DEFAULT_RESTORE_TIMEOUT_SECONDS = 172_800
DEFAULT_POLL_TIMEOUT_SECONDS = 259_200
POLL_INTERVAL_SECONDS = 60
RESTORE_START_GRACE_SECONDS = 120
STUCK_RESTORING_SECONDS = 1_800
RESTORE_IN_PROGRESS_STATES = frozenset(
    {"RESTORING", "RECOVERY_PENDING", "RECOVERING"}
)
SQLCMD_CANDIDATES = (
    Path(r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\SQLCMD.EXE"),
    Path(r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\SQLCMD.EXE"),
)


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"Environment file not found: {path}")
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def env_int(values: dict[str, str], key: str, default: int) -> int:
    raw = values.get(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Config:
    sql_server: str
    database: str
    bak_source: Path
    bak_dest: Path
    data_dir: Path
    log_dir: Path
    log_data_dir: Path
    state_file: Path
    min_age_minutes: int
    restore_timeout_seconds: int
    poll_timeout_seconds: int
    sqlcmd_path: Path

    @classmethod
    def from_env(cls, values: dict[str, str]) -> "Config":
        database = values.get("MSSQL_DB_NAME", "erp_pm").strip() or "erp_pm"
        if not re.fullmatch(r"[A-Za-z0-9_-]+", database):
            raise ValueError("MSSQL_DB_NAME may contain only letters, digits, _ and -")

        log_data_raw = (
            values.get("MSSQL_LOG_DATA_DIR", "").strip()
            or values.get("MSSQL_LOG_STAGING_DIR", "").strip()
            or r"C:/mssql/logdata"
        )
        sqlcmd = resolve_sqlcmd(values.get("MSSQL_SQLCMD", "").strip())
        return cls(
            sql_server=values.get("SQL_SERVER", ".").strip() or ".",
            database=database,
            bak_source=Path(values.get("MSSQL_BAK_SOURCE", r"Z:\\")),
            bak_dest=Path(values.get("MSSQL_BAK_DEST", r"D:\\")),
            data_dir=Path(values.get("MSSQL_DATA_DIR", "D:/mssql/data")),
            log_dir=Path(values.get("MSSQL_LOG_DIR", "D:/mssql/logs")),
            log_data_dir=Path(log_data_raw),
            state_file=Path(values.get("MSSQL_STATE_FILE", "D:/mssql/state.json")),
            min_age_minutes=env_int(values, "BACKUP_MIN_AGE_MINUTES", 5),
            restore_timeout_seconds=env_int(
                values, "RESTORE_TIMEOUT_SECONDS", DEFAULT_RESTORE_TIMEOUT_SECONDS
            ),
            poll_timeout_seconds=env_int(
                values, "RESTORE_POLL_TIMEOUT_SECONDS", DEFAULT_POLL_TIMEOUT_SECONDS
            ),
            sqlcmd_path=sqlcmd,
        )


def resolve_sqlcmd(configured: str) -> Path:
    if configured:
        path = Path(configured)
        if path.is_file():
            return path
        raise FileNotFoundError(f"MSSQL_SQLCMD not found: {configured}")
    which = shutil.which("sqlcmd")
    if which:
        return Path(which)
    for candidate in SQLCMD_CANDIDATES:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("sqlcmd.exe not found; install SQL Server tools or set MSSQL_SQLCMD")


def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"restore_{datetime.now():%Y%m%d}.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # The restore implementation uses module-level logging.* calls, which emit
    # through the root logger. Attach the file handler there so both Django
    # scheduler runs and standalone CLI runs are persisted.
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    expected_path = os.path.normcase(os.path.abspath(log_file))
    already = any(
        isinstance(handler, logging.FileHandler)
        and os.path.normcase(os.path.abspath(handler.baseFilename)) == expected_path
        for handler in root.handlers
    )
    if not already:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # A FileHandler is also a StreamHandler, so only consider non-file streams.
    if not any(
        isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, logging.FileHandler)
        for handler in root.handlers
    ):
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        root.addHandler(console)


def run(
    args: list[str],
    *,
    timeout: int | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    logging.debug("Run: %s", subprocess.list2cmdline(args))
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        output = (result.stderr or result.stdout).strip()
        raise RuntimeError(
            f"Command failed ({result.returncode}): {args[0]}: {output}"
        )
    return result


def sql_string(value: str) -> str:
    return "N'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


def sqlcmd(cfg: Config, query: str, *, timeout: int) -> str:
    args = [
        str(cfg.sqlcmd_path),
        "-S",
        cfg.sql_server,
        "-E",
        "-C",
        "-b",
        "-r",
        "1",
        "-h",
        "-1",
        "-W",
        "-w",
        "65535",
        "-s",
        "|",
        "-Q",
        query,
    ]
    result = run(args, timeout=timeout, check=True)
    return result.stdout


def wait_for_sql(cfg: Config, timeout_seconds: int = 300) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            sqlcmd(cfg, "SET NOCOUNT ON; SELECT 1;", timeout=30)
            logging.info("SQL Server is ready (%s)", cfg.sql_server)
            return
        except Exception as exc:
            last_error = str(exc)
            time.sleep(5)
    raise TimeoutError(f"SQL Server did not become ready: {last_error}")


def fingerprint(path: Path) -> dict[str, object]:
    stat = path.stat()
    return {
        "bak_name": path.name,
        "bak_size": stat.st_size,
        "bak_mtime_ns": stat.st_mtime_ns,
        "bak_mtime": datetime.fromtimestamp(
            stat.st_mtime, tz=timezone.utc
        ).isoformat(),
    }


def same_backup(a: dict[str, object], b: dict[str, object]) -> bool:
    return all(
        a.get(key) == b.get(key)
        for key in ("bak_name", "bak_size", "bak_mtime_ns")
    )


def load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logging.warning("Ignoring unreadable state file: %s", path)
        return {}
    return data if isinstance(data, dict) else {}


def save_state(path: Path, source: dict[str, object]) -> None:
    state = {
        **source,
        "restored_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def ensure_bak_source_accessible(cfg: Config, values: dict[str, str]) -> None:
    """Remount Z:/SMB if the backup source disappeared after reboot/logoff."""
    if cfg.bak_source.exists():
        return
    share = values.get("SMB_SHARE", "").strip() or r"\\srv2\copy1cbase"
    login = values.get("LOGIN", "").strip()
    password = values.get("PASSWORD", "")
    if not login:
        raise FileNotFoundError(
            f"Backup source is not accessible: {cfg.bak_source} "
            f"(and LOGIN is not set to remount {share})"
        )
    logging.warning(
        "Backup source %s missing; remounting %s as %s",
        cfg.bak_source,
        share,
        cfg.bak_source,
    )
    # Map drive letter if bak_source looks like X:\
    drive = str(cfg.bak_source).rstrip("\\/")
    if len(drive) == 2 and drive[1] == ":":
        run(["net", "use", f"{drive}", "/delete", "/y"], check=False, timeout=30)
        result = run(
            [
                "net",
                "use",
                f"{drive}",
                share,
                f"/user:{login}",
                password,
                "/persistent:no",
            ],
            check=False,
            timeout=60,
        )
        if result.returncode != 0 and not cfg.bak_source.exists():
            output = (result.stderr or result.stdout).strip()
            raise RuntimeError(f"Cannot remount {drive} -> {share}: {output}")
    elif not Path(share).exists():
        run(["net", "use", share, "/delete", "/y"], check=False, timeout=30)
        result = run(
            [
                "net",
                "use",
                share,
                f"/user:{login}",
                password,
                "/persistent:no",
            ],
            check=False,
            timeout=60,
        )
        if result.returncode != 0 and not Path(share).exists():
            output = (result.stderr or result.stdout).strip()
            raise RuntimeError(f"Cannot connect to {share}: {output}")
    if not cfg.bak_source.exists():
        raise FileNotFoundError(f"Backup source is not accessible: {cfg.bak_source}")


def latest_backup(share: Path) -> Path:
    if not share.exists():
        raise FileNotFoundError(f"Backup source is not accessible: {share}")
    backups = [path for path in share.glob("*.bak") if path.is_file()]
    if not backups:
        raise FileNotFoundError(f"No .bak files found in {share}")
    return max(backups, key=lambda path: (path.stat().st_mtime_ns, path.name))


def local_erp_pm_backups(dest: Path) -> list[Path]:
    if not dest.exists():
        return []
    return sorted(
        [
            path
            for path in dest.glob("erp_pm*")
            if path.is_file() and path.suffix.lower() == ".bak"
        ],
        key=lambda path: (path.stat().st_mtime_ns, path.name),
    )


def matching_local_backup(dest: Path, source_fp: dict[str, object]) -> Path | None:
    for path in local_erp_pm_backups(dest):
        if same_backup(fingerprint(path), source_fp):
            return path
    # Same name + size is enough when mtime was altered by a previous copy tool.
    name = str(source_fp.get("bak_name") or "")
    size = int(source_fp.get("bak_size") or 0)
    for path in local_erp_pm_backups(dest):
        if path.name == name and path.stat().st_size == size:
            return path
    return None


def ensure_source_is_complete(path: Path, min_age_minutes: int) -> None:
    age_seconds = time.time() - path.stat().st_mtime
    minimum = min_age_minutes * 60
    if age_seconds < minimum:
        raise RuntimeError(
            f"Newest backup {path.name} is only {age_seconds / 60:.1f} minutes old; "
            f"wait at least {min_age_minutes} minutes"
        )


def ensure_directories(cfg: Config) -> None:
    for path in (cfg.data_dir, cfg.log_dir, cfg.log_data_dir, cfg.bak_dest):
        path.mkdir(parents=True, exist_ok=True)
    cfg.state_file.parent.mkdir(parents=True, exist_ok=True)


def free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def partial_copy_matches(source: Path, partial: Path, partial_size: int) -> bool:
    if partial_size == 0:
        return True
    probe_size = min(4 * 1024**2, partial_size)
    offsets = {0, max(0, partial_size - probe_size)}
    with source.open("rb", buffering=0) as src, partial.open("rb", buffering=0) as dst:
        for offset in offsets:
            src.seek(offset)
            dst.seek(offset)
            if src.read(probe_size) != dst.read(probe_size):
                return False
    return True


def copy_backup_python(
    source: Path, destination: Path, expected: dict[str, object]
) -> None:
    expected_size = int(expected["bak_size"])
    partial = destination.with_name(destination.name + ".partial")
    resume_at = partial.stat().st_size if partial.exists() else 0
    if resume_at > expected_size or not partial_copy_matches(
        source, partial, resume_at
    ):
        logging.warning("Discarding invalid partial copy: %s", partial)
        partial.unlink(missing_ok=True)
        resume_at = 0

    copied = resume_at
    session_copied = 0
    last_log = time.monotonic()
    started = time.monotonic()
    if resume_at:
        logging.info(
            "Resuming copy at %.2f/%.2f GiB: %s",
            resume_at / 1024**3,
            expected_size / 1024**3,
            partial,
        )
    else:
        logging.info(
            "Copying %.2f GiB from %s to %s",
            expected_size / 1024**3,
            source,
            destination,
        )

    mode = "r+b" if partial.exists() else "wb"
    with source.open("rb", buffering=0) as src, partial.open(
        mode, buffering=0
    ) as dst:
        src.seek(resume_at)
        dst.seek(resume_at)
        while True:
            chunk = src.read(COPY_BUFFER_BYTES)
            if not chunk:
                break
            written = dst.write(chunk)
            if written != len(chunk):
                raise RuntimeError(
                    f"Short write while copying backup: {written}/{len(chunk)} bytes"
                )
            copied += written
            session_copied += written
            now = time.monotonic()
            if now - last_log >= COPY_LOG_INTERVAL_SECONDS:
                dst.flush()
                os.fsync(dst.fileno())
                percent = copied * 100 / expected_size if expected_size else 100
                speed = session_copied / max(now - started, 0.001) / 1024**2
                logging.info("Copy progress: %.1f%% (%.1f MiB/s)", percent, speed)
                last_log = now
        dst.flush()
        os.fsync(dst.fileno())
    shutil.copystat(source, partial)

    if copied != expected_size:
        raise RuntimeError(f"Copied {copied} bytes, expected {expected_size}")
    after = fingerprint(source)
    if not same_backup(expected, after):
        partial.unlink(missing_ok=True)
        raise RuntimeError("Source backup changed while it was being copied")
    os.replace(partial, destination)
    logging.info("Backup copy completed: %s", destination)


def copy_backup(source: Path, destination: Path, expected: dict[str, object]) -> Path:
    expected_size = int(expected["bak_size"])
    if destination.exists() and destination.stat().st_size == expected_size:
        logging.info("Using existing complete local copy: %s", destination)
        return destination
    # Need bak size free (+ small margin). Old local bak should already be deleted.
    if free_bytes(destination.parent) < expected_size + 5 * 1024**3:
        raise RuntimeError(
            "Not enough free space on D: to copy backup; "
            f"need ~{expected_size / 1024**3:.1f} GiB + 5 GiB margin"
        )
    copy_backup_python(source, destination, expected)
    return destination


@dataclass(frozen=True)
class BackupFile:
    logical_name: str
    file_type: str
    size: int


def read_backup_files(cfg: Config, bak_path: Path) -> list[BackupFile]:
    output = sqlcmd(
        cfg,
        f"SET NOCOUNT ON; RESTORE FILELISTONLY FROM DISK={sql_string(str(bak_path))};",
        timeout=1800,
    )
    files: list[BackupFile] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("("):
            continue
        columns = [part.strip() for part in line.split("|")]
        if len(columns) < 5 or columns[2] not in {"D", "L", "S", "F"}:
            continue
        try:
            size = int(columns[4])
        except ValueError:
            size = 0
        files.append(BackupFile(columns[0], columns[2], size))
    if not files:
        raise RuntimeError(f"RESTORE FILELISTONLY returned no files:\n{output[:2000]}")
    logging.info(
        "Backup contains %d files, allocated size %.2f GiB",
        len(files),
        sum(item.size for item in files) / 1024**3,
    )
    return files


def _next_free_path(directory: Path, prefix: str, extension: str) -> Path:
    """Pick directory/prefix_N.ext, skipping files that still exist (incl. locked orphans)."""
    index = 1
    while True:
        candidate = directory / f"{prefix}_{index}.{extension}"
        if not candidate.exists():
            return candidate
        index += 1
        if index > 100:
            raise RuntimeError(
                f"Cannot find free path for {prefix}*.{extension} under {directory}"
            )


def restore_moves(cfg: Config, files: Iterable[BackupFile]) -> list[str]:
    moves: list[str] = []
    data_index = 0
    for item in files:
        if item.file_type == "L":
            target_path = _next_free_path(cfg.log_data_dir, f"{cfg.database}_log", "ldf")
            target = str(target_path)
        elif item.file_type == "D":
            data_index += 1
            extension = "mdf" if data_index == 1 else "ndf"
            target_path = _next_free_path(
                cfg.data_dir, f"{cfg.database}_data", extension
            )
            # Keep first data file named *_data_1.mdf when free; _next_free_path
            # already skipped occupied indexes.
            target = str(target_path)
        else:
            raise RuntimeError(
                f"Unsupported SQL backup file type {item.file_type!r} "
                f"for logical file {item.logical_name!r}"
            )
        moves.append(
            f"MOVE {sql_string(item.logical_name)} TO {sql_string(target)}"
        )
        logging.info("RESTORE MOVE %s -> %s", item.logical_name, target)
    return moves


def database_state_desc(cfg: Config) -> str | None:
    query = (
        "SET NOCOUNT ON; SELECT state_desc FROM sys.databases "
        f"WHERE name={sql_string(cfg.database)};"
    )
    try:
        output = sqlcmd(cfg, query, timeout=30)
    except Exception:
        return None
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if line and not line.startswith("(") and "rows affected" not in line.lower():
            return line
    return None


def database_is_online(cfg: Config) -> bool:
    return database_state_desc(cfg) == "ONLINE"


@dataclass(frozen=True)
class RestoreProgress:
    command: str
    percent_complete: float
    wait_type: str
    eta_seconds: int | None


def active_restore_progress(cfg: Config) -> RestoreProgress | None:
    query = (
        "SET NOCOUNT ON; "
        "SELECT TOP 1 "
        "r.command, "
        "CONVERT(varchar(32), r.percent_complete), "
        "ISNULL(r.wait_type, N''), "
        "CONVERT(varchar(32), r.estimated_completion_time / 1000) "
        "FROM sys.dm_exec_requests AS r "
        "WHERE r.command LIKE N'%RESTORE%' "
        "ORDER BY r.percent_complete DESC, r.start_time;"
    )
    try:
        output = sqlcmd(cfg, query, timeout=30)
    except Exception:
        return None
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("(") or "|" not in line:
            continue
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 3:
            continue
        try:
            percent = float(parts[1])
        except ValueError:
            percent = 0.0
        try:
            eta = int(parts[3]) if len(parts) > 3 and parts[3] else None
        except ValueError:
            eta = None
        return RestoreProgress(parts[0], percent, parts[2], eta)
    return None


def restore_in_progress(cfg: Config) -> bool:
    state = database_state_desc(cfg)
    if state in RESTORE_IN_PROGRESS_STATES:
        return True
    return active_restore_progress(cfg) is not None


def ensure_restore_space(cfg: Config, files: Iterable[BackupFile]) -> None:
    data_required = (
        sum(item.size for item in files if item.file_type == "D") + 10 * 1024**3
    )
    log_required = (
        sum(item.size for item in files if item.file_type == "L") + 5 * 1024**3
    )
    reusable_data = sum(
        path.stat().st_size
        for path in cfg.data_dir.glob(f"{cfg.database}_data_*")
        if path.is_file()
    )
    reusable_log = sum(
        path.stat().st_size
        for path in cfg.log_data_dir.glob(f"{cfg.database}_log_*")
        if path.is_file()
    )
    data_free = free_bytes(cfg.data_dir) + reusable_data
    log_free = free_bytes(cfg.log_data_dir) + reusable_log
    if data_free < data_required:
        raise RuntimeError(
            "Not enough D: space for restored data files; "
            f"required approximately {data_required / 1024**3:.1f} GiB, "
            f"available {data_free / 1024**3:.1f} GiB"
        )
    if log_free < log_required:
        raise RuntimeError(
            "Not enough C: space for restored transaction log files; "
            f"required approximately {log_required / 1024**3:.1f} GiB, "
            f"available {log_free / 1024**3:.1f} GiB"
        )


def delete_local_erp_pm_baks(dest: Path, *, keep: Path | None = None) -> None:
    keep_resolved = keep.resolve() if keep is not None else None
    for path in local_erp_pm_backups(dest):
        if keep_resolved is not None and path.resolve() == keep_resolved:
            continue
        try:
            path.unlink()
            logging.info("Deleted local backup: %s", path)
        except OSError:
            logging.exception("Could not delete local backup: %s", path)
    for path in dest.glob("erp_pm*.bak.partial"):
        try:
            path.unlink()
            logging.info("Deleted partial backup: %s", path)
        except OSError:
            logging.exception("Could not delete partial backup: %s", path)


def drop_database(cfg: Config) -> None:
    state = database_state_desc(cfg)
    if state is None:
        logging.info("Database %s does not exist; skip DROP", cfg.database)
        return
    progress = active_restore_progress(cfg)
    if progress is not None:
        raise RuntimeError(
            f"Refusing to DROP {cfg.database}: active RESTORE in progress "
            f"({progress.command} {progress.percent_complete}%)"
        )
    if state in RESTORE_IN_PROGRESS_STATES:
        logging.warning(
            "Database %s is stuck in %s with no active RESTORE; forcing DROP",
            cfg.database,
            state,
        )
    database = sql_identifier(cfg.database)
    logging.info("Dropping database %s (was %s)", cfg.database, state)
    # RESTORING DBs often reject SINGLE_USER; try DROP directly first.
    try:
        sqlcmd(
            cfg,
            "SET NOCOUNT ON; "
            f"IF DB_ID({sql_string(cfg.database)}) IS NOT NULL "
            f"DROP DATABASE {database};",
            timeout=600,
        )
    except Exception:
        logging.exception("Direct DROP failed; retrying with SINGLE_USER")
        sqlcmd(
            cfg,
            "SET NOCOUNT ON; "
            f"IF DB_ID({sql_string(cfg.database)}) IS NOT NULL "
            f"BEGIN "
            f"ALTER DATABASE {database} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
            f"DROP DATABASE {database}; "
            f"END;",
            timeout=600,
        )
    if database_state_desc(cfg) is not None:
        raise RuntimeError(f"Database {cfg.database} still exists after DROP")
    logging.info("Database %s dropped", cfg.database)


def purge_orphan_files(cfg: Config) -> None:
    patterns = (
        (cfg.data_dir, f"{cfg.database}_data_*"),
        (cfg.data_dir, f"{cfg.database}.*"),
        (cfg.log_data_dir, f"{cfg.database}_log_*"),
        (cfg.log_data_dir, f"{cfg.database}.*"),
    )
    seen: set[Path] = set()
    for directory, pattern in patterns:
        if not directory.exists():
            continue
        for path in directory.glob(pattern):
            if not path.is_file() or path in seen:
                continue
            # Keep only SQL data/log extensions.
            if path.suffix.lower() not in {".mdf", ".ndf", ".ldf"}:
                continue
            seen.add(path)
            try:
                path.unlink()
                logging.info("Purged orphan DB file: %s", path)
            except OSError:
                logging.exception("Could not purge orphan DB file: %s", path)


def build_restore_sql(cfg: Config, bak_path: Path, files: Iterable[BackupFile]) -> str:
    database = sql_identifier(cfg.database)
    moves = restore_moves(cfg, files)
    return (
        "SET NOCOUNT ON; "
        f"IF DB_ID({sql_string(cfg.database)}) IS NOT NULL "
        f"ALTER DATABASE {database} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
        f"RESTORE DATABASE {database} FROM DISK={sql_string(str(bak_path))} "
        f"WITH REPLACE, RECOVERY, STATS=5, {', '.join(moves)}; "
        f"ALTER DATABASE {database} SET MULTI_USER;"
    )


def launch_restore_sql(cfg: Config, restore_sql: str) -> None:
    progress = active_restore_progress(cfg)
    state = database_state_desc(cfg)
    if progress is not None:
        logging.info(
            "RESTORE already running for %s (%.1f%%); will not issue WITH REPLACE",
            cfg.database,
            progress.percent_complete,
        )
        return
    if state in RESTORE_IN_PROGRESS_STATES:
        logging.warning(
            "Database %s is %s but no active RESTORE request; "
            "treating as stuck and re-issuing WITH REPLACE",
            cfg.database,
            state,
        )

    log_path = cfg.log_dir / f"sqlcmd_restore_{datetime.now():%Y%m%d_%H%M%S}.log"
    args = [
        str(cfg.sqlcmd_path),
        "-S",
        cfg.sql_server,
        "-E",
        "-C",
        "-b",
        "-r",
        "1",
        "-Q",
        restore_sql,
    ]
    logging.info(
        "Launching RESTORE via sqlcmd (log %s); survives client disconnect on server side",
        log_path,
    )
    with log_path.open("w", encoding="utf-8") as log_file:
        creationflags = 0
        if os.name == "nt":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
        proc = subprocess.Popen(
            args,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
        )
    grace_deadline = time.monotonic() + RESTORE_START_GRACE_SECONDS
    while time.monotonic() < grace_deadline:
        if database_is_online(cfg) or restore_in_progress(cfg):
            return
        if proc.poll() is not None:
            break
        time.sleep(5)

    if database_is_online(cfg) or restore_in_progress(cfg):
        return

    # Fallback: attached sqlcmd with long timeout.
    if proc.poll() is None:
        logging.info("Waiting for launched sqlcmd pid=%s", proc.pid)
        try:
            proc.wait(timeout=cfg.restore_timeout_seconds)
        except subprocess.TimeoutExpired:
            if restore_in_progress(cfg) or database_is_online(cfg):
                logging.warning(
                    "sqlcmd still running after client budget; switching to poll"
                )
                return
            proc.kill()
            raise RuntimeError("RESTORE sqlcmd timed out with no server-side progress")
        if database_is_online(cfg) or restore_in_progress(cfg):
            return
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
        raise RuntimeError(f"RESTORE sqlcmd exited without ONLINE:\n{tail}")

    # Process exited before restore became visible — try attached once.
    logging.warning("Detached sqlcmd exited early; trying attached RESTORE")
    try:
        output = sqlcmd(cfg, restore_sql, timeout=cfg.restore_timeout_seconds)
        if output.strip():
            logging.info("SQL restore output:\n%s", output.strip())
    except subprocess.TimeoutExpired:
        if restore_in_progress(cfg) or database_is_online(cfg):
            logging.warning("Attached sqlcmd timed out; switching to poll-until-ONLINE")
            return
        raise RuntimeError("RESTORE timed out and no server-side restore is running")


def poll_until_online(cfg: Config) -> None:
    budget = cfg.poll_timeout_seconds
    deadline = time.monotonic() + budget
    stuck_since: float | None = None
    logging.info(
        "Polling for %s to become ONLINE (deadline %.1f h)",
        cfg.database,
        budget / 3600,
    )
    while time.monotonic() < deadline:
        state = database_state_desc(cfg)
        progress = active_restore_progress(cfg)
        if progress is None:
            logging.info(
                "Restore poll: state=%s percent=n/a wait=n/a",
                state or "MISSING",
            )
        else:
            eta = (
                f"{progress.eta_seconds}s"
                if progress.eta_seconds is not None
                else "n/a"
            )
            logging.info(
                "Restore poll: state=%s command=%s percent=%.1f wait=%s eta=%s",
                state or "MISSING",
                progress.command,
                progress.percent_complete,
                progress.wait_type or "none",
                eta,
            )

        if state == "ONLINE" and progress is None:
            logging.info("Database %s is ONLINE", cfg.database)
            return

        if state in RESTORE_IN_PROGRESS_STATES or progress is not None:
            if state in RESTORE_IN_PROGRESS_STATES and progress is None:
                if stuck_since is None:
                    stuck_since = time.monotonic()
                elif time.monotonic() - stuck_since >= STUCK_RESTORING_SECONDS:
                    raise RuntimeError(
                        f"Database {cfg.database} stuck in {state} for "
                        f"{STUCK_RESTORING_SECONDS // 60} minutes with no active RESTORE"
                    )
            else:
                stuck_since = None
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        if state is None:
            stuck_since = None
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        raise RuntimeError(
            f"Database {cfg.database} entered unexpected state {state!r} "
            "while waiting for restore to finish"
        )

    raise TimeoutError(
        f"Timed out after {budget / 3600:.1f} h waiting for "
        f"{cfg.database} to become ONLINE"
    )


def restore_database(cfg: Config, bak_path: Path) -> None:
    progress = active_restore_progress(cfg)
    if progress is not None:
        logging.info(
            "Database %s is already restoring (%.1f%%); polling instead of WITH REPLACE",
            cfg.database,
            progress.percent_complete,
        )
        poll_until_online(cfg)
        if not database_is_online(cfg):
            raise RuntimeError(f"Database {cfg.database} is not ONLINE after poll")
        return

    state = database_state_desc(cfg)
    if state in RESTORE_IN_PROGRESS_STATES:
        logging.warning(
            "Database %s stuck in %s with no active RESTORE; dropping before retry",
            cfg.database,
            state,
        )
        drop_database(cfg)
        purge_orphan_files(cfg)

    files = read_backup_files(cfg, bak_path)
    ensure_restore_space(cfg, files)
    restore_sql = build_restore_sql(cfg, bak_path, files)
    logging.info("Restoring database %s from %s", cfg.database, bak_path)
    launch_restore_sql(cfg, restore_sql)
    if not database_is_online(cfg):
        poll_until_online(cfg)
    if not database_is_online(cfg):
        raise RuntimeError(f"Database {cfg.database} is not ONLINE after restore")
    logging.info(
        "Database %s restored and ONLINE (data on D:, logs on C:\\mssql\\logdata)",
        cfg.database,
    )


class ProcessLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self) -> "ProcessLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        if os.name == "nt":
            self.handle.seek(0)
            if self.handle.tell() == self.handle.seek(0, os.SEEK_END):
                self.handle.write(b"0")
                self.handle.flush()
            self.handle.seek(0)
            try:
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                self.handle.close()
                raise RuntimeError("Another restore process is already running") from exc
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle is None:
            return
        if os.name == "nt":
            self.handle.seek(0)
            try:
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        self.handle.close()


class SourceBackupLock:
    _GENERIC_READ = 0x80000000
    _FILE_SHARE_READ = 0x00000001
    _OPEN_EXISTING = 3
    _FILE_ATTRIBUTE_NORMAL = 0x80
    _INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value if os.name == "nt" else -1

    def __init__(self, path: Path):
        self.path = path
        self._handle: int | object | None = None

    def __enter__(self) -> "SourceBackupLock":
        if os.name != "nt":
            self._handle = self.path.open("rb")
            return self
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateFileW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        kernel32.CreateFileW.restype = wintypes.HANDLE
        handle = kernel32.CreateFileW(
            str(self.path),
            self._GENERIC_READ,
            self._FILE_SHARE_READ,
            None,
            self._OPEN_EXISTING,
            self._FILE_ATTRIBUTE_NORMAL,
            None,
        )
        if handle == self._INVALID_HANDLE_VALUE or int(handle) == -1:
            error = ctypes.get_last_error()
            raise RuntimeError(
                f"Cannot lock source backup {self.path}: WinError {error}"
            )
        self._handle = int(handle)
        logging.info("Source bak locked: %s", self.path)
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt" and isinstance(self._handle, int):
                ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(
                    wintypes.HANDLE(self._handle)
                )
            else:
                self._handle.close()  # type: ignore[union-attr]
        except Exception:
            logging.exception("Failed to release source bak lock: %s", self.path)
        finally:
            self._handle = None


def restore_is_disabled(values: dict[str, str], cfg: Config) -> Path | str | None:
    if values.get("MSSQL_RESTORE_DISABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return "MSSQL_RESTORE_DISABLED"
    for path in (
        Path(r"D:\mssql\RESTORE_DISABLED"),
        cfg.state_file.parent / "RESTORE_DISABLED",
        SCRIPT_DIR / "RESTORE_DISABLED",
    ):
        if path.exists():
            return path
    return None


def run_restore(*, force: bool = False) -> dict[str, object]:
    """Run one native restore cycle.

    Returns a dict with at least ``status``:
    - ``disabled`` — kill-switch active
    - ``skipped`` — local D:\\erp_pm*.bak matches Z:\\ latest and DB is ONLINE
    - ``restored`` — restore completed (or finished an in-progress restore)
    - ``already_running`` — process lock held by another run
    """
    values = load_env(ENV_FILE)
    cfg = Config.from_env(values)
    ensure_directories(cfg)
    setup_logging(cfg.log_dir)

    disabled = restore_is_disabled(values, cfg)
    if disabled is not None:
        logging.error("Restore is disabled (%s). Refusing to start.", disabled)
        return {"status": "disabled", "reason": str(disabled)}

    lock_path = cfg.state_file.with_name("restore.lock")
    try:
        with ProcessLock(lock_path):
            return _run_restore_locked(cfg, values, force=force)
    except RuntimeError as exc:
        if "already running" in str(exc).lower():
            logging.warning("%s", exc)
            return {"status": "already_running", "reason": str(exc)}
        raise


def _run_restore_locked(
    cfg: Config, values: dict[str, str], *, force: bool
) -> dict[str, object]:
    wait_for_sql(cfg)
    ensure_bak_source_accessible(cfg, values)
    source = latest_backup(cfg.bak_source)
    ensure_source_is_complete(source, cfg.min_age_minutes)
    current = fingerprint(source)
    current["database"] = cfg.database
    logging.info(
        "Newest backup on %s: %s, %.2f GiB, modified %s",
        cfg.bak_source,
        source.name,
        int(current["bak_size"]) / 1024**3,
        current["bak_mtime"],
    )

    with SourceBackupLock(source):
        local_match = matching_local_backup(cfg.bak_dest, current)

        progress = active_restore_progress(cfg)
        if progress is not None:
            logging.info(
                "In-progress restore detected (%.1f%%); polling until ONLINE",
                progress.percent_complete,
            )
            poll_until_online(cfg)
            save_state(cfg.state_file, current)
            return {
                "status": "restored",
                "bak_name": source.name,
                "reason": "finished_in_progress",
            }
        stuck_state = database_state_desc(cfg)
        if stuck_state in RESTORE_IN_PROGRESS_STATES:
            logging.warning(
                "Stuck %s with no active RESTORE; will DROP and re-restore",
                stuck_state,
            )

        if (
            not force
            and local_match is not None
            and database_is_online(cfg)
        ):
            logging.info(
                "Local bak %s matches Z:\\ source and %s is ONLINE; nothing to do",
                local_match.name,
                cfg.database,
            )
            save_state(cfg.state_file, current)
            return {
                "status": "skipped",
                "bak_name": source.name,
                "local_bak": str(local_match),
            }

        # Local bak already matches but DB is missing/offline → restore only.
        if local_match is not None and not force:
            logging.info(
                "Local bak %s matches source but database is not ONLINE; restoring",
                local_match.name,
            )
            restore_database(cfg, local_match)
            save_state(cfg.state_file, current)
            return {
                "status": "restored",
                "bak_name": source.name,
                "local_bak": str(local_match),
                "reason": "restore_existing_local",
            }

        # Full cycle: delete D:\erp_pm*.bak, DROP DB, purge files, copy, restore.
        logging.info("Starting full native restore cycle for %s", source.name)
        delete_local_erp_pm_baks(cfg.bak_dest)
        drop_database(cfg)
        purge_orphan_files(cfg)

        destination = cfg.bak_dest / source.name
        local_bak = copy_backup(source, destination, current)
        restore_database(cfg, local_bak)
        save_state(cfg.state_file, current)
        logging.info("Native daily restore completed successfully")
        return {
            "status": "restored",
            "bak_name": source.name,
            "local_bak": str(local_bak),
            "reason": "full_cycle",
        }


def main() -> int:
    force = os.getenv("MSSQL_RESTORE_FORCE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    try:
        result = run_restore(force=force)
    except Exception:
        logging.exception("Native daily restore failed")
        return 1
    status = str(result.get("status") or "")
    if status == "disabled":
        print(f"Restore is disabled ({result.get('reason')}).")
        return 2
    if status == "already_running":
        print(result.get("reason") or "Another restore is already running")
        return 3
    if status == "skipped":
        print(f"Skipped: {result.get('bak_name')} already on D: and ONLINE")
        return 0
    if status == "restored":
        print(f"Restored: {result.get('bak_name')}")
        return 0
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
