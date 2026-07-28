r"""Restore the newest 1C SQL Server backup into a Docker SQL Server.

The source is an SMB share. Data files (.mdf/.ndf) are restored to D:;
transaction logs (.ldf) stay permanently on C:. The script is idempotent: a
successfully restored source fingerprint is saved to D:\mssql\state.json and
is not restored twice.
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
COMPOSE_FILE = SCRIPT_DIR / "docker-compose.yml"
CONTAINER_NAME = "dashboard-erp-mssql"
CONTAINER_BACKUP_DIR = "/var/opt/mssql/backup"
CONTAINER_DATA_DIR = "/var/opt/mssql/data"
CONTAINER_LOG_DIR = "/var/opt/mssql/log"
COPY_BUFFER_BYTES = 16 * 1024 * 1024
COPY_LOG_INTERVAL_SECONDS = 30
DEFAULT_RESTORE_TIMEOUT_SECONDS = 172_800  # 48h sqlcmd / long-op client budget
DEFAULT_POLL_TIMEOUT_SECONDS = 259_200  # 72h poll-until-ONLINE ceiling
POLL_INTERVAL_SECONDS = 60
RESTORE_START_GRACE_SECONDS = 120
STUCK_RESTORING_SECONDS = 1_800
RESTORE_IN_PROGRESS_STATES = frozenset(
    {"RESTORING", "RECOVERY_PENDING", "RECOVERING"}
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
    login: str
    password: str
    smb_share: Path
    sa_password: str
    database: str
    host_port: int
    data_dir: Path
    incoming_dir: Path
    log_dir: Path
    log_data_dir: Path
    state_file: Path
    min_age_minutes: int
    restore_timeout_seconds: int
    poll_timeout_seconds: int

    @classmethod
    def from_env(cls, values: dict[str, str]) -> "Config":
        required = ["LOGIN", "PASSWORD", "MSSQL_SA_PASSWORD"]
        missing = [key for key in required if not values.get(key, "").strip()]
        if missing:
            raise ValueError(f"Missing required .env values: {', '.join(missing)}")

        database = values.get("MSSQL_DB_NAME", "erp_pm").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]+", database):
            raise ValueError("MSSQL_DB_NAME may contain only letters, digits, _ and -")

        # Permanent SQL .ldf location on C:. Accept legacy MSSQL_LOG_STAGING_DIR.
        log_data_raw = (
            values.get("MSSQL_LOG_DATA_DIR", "").strip()
            or values.get("MSSQL_LOG_STAGING_DIR", "").strip()
            or "C:/mssql/log"
        )

        return cls(
            login=values["LOGIN"].strip(),
            password=values["PASSWORD"],
            smb_share=Path(values.get("SMB_SHARE", r"\\srv2\copy1cbase")),
            sa_password=values["MSSQL_SA_PASSWORD"],
            database=database,
            host_port=env_int(values, "MSSQL_HOST_PORT", 1433),
            data_dir=Path(values.get("MSSQL_DATA_DIR", "D:/mssql/data")),
            incoming_dir=Path(values.get("MSSQL_INCOMING_DIR", "D:/mssql/incoming")),
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
        )


def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"restore_{datetime.now():%Y%m%d}.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def run(
    args: list[str],
    *,
    timeout: int | None = None,
    check: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    logging.debug("Run: %s", subprocess.list2cmdline(args))
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=env,
        check=False,
    )
    if check and result.returncode != 0:
        output = (result.stderr or result.stdout).strip()
        raise RuntimeError(
            f"Command failed ({result.returncode}): {args[0]}: {output}"
        )
    return result


def connect_share(cfg: Config) -> None:
    share = str(cfg.smb_share)
    run(["net", "use", share, "/delete", "/y"], check=False, timeout=30)
    result = run(
        ["net", "use", share, f"/user:{cfg.login}", cfg.password, "/persistent:no"],
        check=False,
        timeout=60,
    )
    if result.returncode != 0 and not cfg.smb_share.exists():
        output = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"Cannot connect to {share}: {output}")
    if not cfg.smb_share.exists():
        raise RuntimeError(f"SMB share is not accessible after net use: {share}")
    logging.info("SMB share is accessible: %s", share)


def latest_backup(share: Path) -> Path:
    backups = [path for path in share.glob("*.bak") if path.is_file()]
    if not backups:
        raise FileNotFoundError(f"No .bak files found in {share}")
    return max(backups, key=lambda path: (path.stat().st_mtime_ns, path.name))


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


def load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logging.warning("Ignoring unreadable state file: %s", path)
        return {}
    return data if isinstance(data, dict) else {}


def same_backup(state: dict[str, object], current: dict[str, object]) -> bool:
    return all(
        state.get(key) == current.get(key)
        for key in ("bak_name", "bak_size", "bak_mtime_ns")
    )


def ensure_source_is_complete(path: Path, min_age_minutes: int) -> None:
    stat = path.stat()
    age_seconds = time.time() - stat.st_mtime
    minimum = min_age_minutes * 60
    if age_seconds < minimum:
        raise RuntimeError(
            f"Newest backup {path.name} is only {age_seconds / 60:.1f} minutes old; "
            f"wait at least {min_age_minutes} minutes"
        )


def ensure_directories(cfg: Config) -> None:
    for path in (
        cfg.data_dir,
        cfg.incoming_dir,
        cfg.log_dir,
        cfg.log_data_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
    cfg.state_file.parent.mkdir(parents=True, exist_ok=True)


def free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def partial_copy_matches(source: Path, partial: Path, partial_size: int) -> bool:
    """Verify the beginning and current tail before resuming a partial copy."""
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
        logging.warning("Discarding invalid partial Python copy: %s", partial)
        partial.unlink(missing_ok=True)
        resume_at = 0

    copied = resume_at
    session_copied = 0
    last_log = time.monotonic()
    started = time.monotonic()
    if resume_at:
        logging.info(
            "Resuming Python copy at %.2f/%.2f GiB: %s",
            resume_at / 1024**3,
            expected_size / 1024**3,
            partial,
        )
    else:
        logging.info(
            "Copying %.2f GiB with Python from %s to %s",
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
                logging.info(
                    "Copy progress: %.1f%% (%.1f MiB/s)", percent, speed
                )
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


def verify_and_repair_python_copy(
    source: Path, destination: Path, expected: dict[str, object]
) -> None:
    """Byte-compare the SMB source and repair any differing local chunks."""
    expected_size = int(expected["bak_size"])
    verified_file = destination.with_suffix(destination.suffix + ".verified.json")
    if verified_file.exists():
        try:
            verified = json.loads(verified_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            verified = {}
        if isinstance(verified, dict) and same_backup(verified, expected):
            logging.info("Local Python copy already has byte-verification metadata")
            return

    compared = 0
    repaired_chunks = 0
    started = time.monotonic()
    last_log = started
    logging.info("Byte-verifying local backup against SMB source with Python")
    with source.open("rb", buffering=0) as src, destination.open(
        "r+b", buffering=0
    ) as dst:
        while compared < expected_size:
            source_chunk = src.read(min(COPY_BUFFER_BYTES, expected_size - compared))
            local_chunk = dst.read(len(source_chunk))
            if not source_chunk:
                break
            if local_chunk != source_chunk:
                dst.seek(compared)
                written = dst.write(source_chunk)
                if written != len(source_chunk):
                    raise RuntimeError(
                        f"Short repair write: {written}/{len(source_chunk)} bytes"
                    )
                repaired_chunks += 1
            compared += len(source_chunk)
            now = time.monotonic()
            if now - last_log >= COPY_LOG_INTERVAL_SECONDS:
                dst.flush()
                os.fsync(dst.fileno())
                logging.info(
                    "Verification progress: %.1f%%; repaired chunks: %d",
                    compared * 100 / expected_size,
                    repaired_chunks,
                )
                last_log = now
        dst.flush()
        os.fsync(dst.fileno())

    if compared != expected_size or destination.stat().st_size != expected_size:
        raise RuntimeError(
            f"Verified {compared} bytes, expected {expected_size}"
        )
    after = fingerprint(source)
    if not same_backup(expected, after):
        verified_file.unlink(missing_ok=True)
        raise RuntimeError("Source backup changed during byte verification")
    verified_file.write_text(
        json.dumps(expected, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logging.info(
        "Python byte-verification completed in %.1f minutes; repaired chunks: %d",
        (time.monotonic() - started) / 60,
        repaired_chunks,
    )


def copy_backup(source: Path, destination: Path, expected: dict[str, object]) -> None:
    expected_size = int(expected["bak_size"])
    robocopy_staging = destination.parent / ".staging"
    if robocopy_staging.exists():
        logging.info("Removing obsolete robocopy staging directory")
        shutil.rmtree(robocopy_staging)
    if destination.exists() and destination.stat().st_size == expected_size:
        logging.info("Using existing complete local copy: %s", destination)
        verify_and_repair_python_copy(source, destination, expected)
        return
    if free_bytes(destination.parent) < expected_size + 10 * 1024**3:
        raise RuntimeError(
            "Not enough free space to copy backup; at least backup size + 10 GiB is required"
        )
    copy_backup_python(source, destination, expected)
    verify_and_repair_python_copy(source, destination, expected)


def compose_args(cfg: Config, *args: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--project-directory",
        str(SCRIPT_DIR),
        "--env-file",
        str(ENV_FILE),
        "-f",
        str(COMPOSE_FILE),
        *args,
    ]


def start_sql_server(cfg: Config) -> None:
    logging.info("Starting SQL Server container")
    run(compose_args(cfg, "up", "-d"), timeout=1800)


def sqlcmd_shell_command() -> str:
    return (
        "SQLCMD=/opt/mssql-tools18/bin/sqlcmd; "
        "[ -x \"$SQLCMD\" ] || SQLCMD=/opt/mssql-tools/bin/sqlcmd; "
        "exec \"$SQLCMD\" -S localhost -U sa -C -b -r 1 "
        "-h -1 -W -w 65535 -s '|' -Q \"$1\""
    )


def sqlcmd_command(query: str, *, detached: bool = False) -> list[str]:
    args = ["docker", "exec"]
    if detached:
        args.append("-d")
    args.extend(
        [
            "-e",
            "SQLCMDPASSWORD",
            CONTAINER_NAME,
            "/bin/bash",
            "-lc",
            sqlcmd_shell_command(),
            "sqlcmd-wrapper",
            query,
        ]
    )
    return args


def sqlcmd(cfg: Config, query: str, *, timeout: int) -> str:
    process_env = os.environ.copy()
    process_env["SQLCMDPASSWORD"] = cfg.sa_password
    result = run(
        sqlcmd_command(query),
        timeout=timeout,
        check=True,
        env=process_env,
    )
    return result.stdout


def sqlcmd_detached(cfg: Config, query: str) -> None:
    """Start sqlcmd inside the container without tying it to this process."""
    process_env = os.environ.copy()
    process_env["SQLCMDPASSWORD"] = cfg.sa_password
    run(
        sqlcmd_command(query, detached=True),
        timeout=60,
        check=True,
        env=process_env,
    )


def wait_for_sql(cfg: Config, timeout_seconds: int = 900) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            sqlcmd(cfg, "SET NOCOUNT ON; SELECT 1;", timeout=30)
            logging.info("SQL Server is ready")
            return
        except Exception as exc:  # server startup produces several transient errors
            last_error = str(exc)
            time.sleep(10)
    raise TimeoutError(f"SQL Server did not become ready: {last_error}")


def sql_string(value: str) -> str:
    return "N'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


@dataclass(frozen=True)
class BackupFile:
    logical_name: str
    file_type: str
    size: int


def read_backup_files(cfg: Config, backup_name: str) -> list[BackupFile]:
    container_path = f"{CONTAINER_BACKUP_DIR}/{backup_name}"
    output = sqlcmd(
        cfg,
        f"SET NOCOUNT ON; RESTORE FILELISTONLY FROM DISK={sql_string(container_path)};",
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


def restore_moves(database: str, files: Iterable[BackupFile]) -> list[str]:
    moves: list[str] = []
    data_index = 0
    log_index = 0
    for item in files:
        if item.file_type == "L":
            log_index += 1
            filename = f"{database}_log_{log_index}.ldf"
            container_dir = CONTAINER_LOG_DIR
        elif item.file_type == "D":
            data_index += 1
            extension = "mdf" if data_index == 1 else "ndf"
            filename = f"{database}_data_{data_index}.{extension}"
            container_dir = CONTAINER_DATA_DIR
        else:
            raise RuntimeError(
                f"Unsupported SQL backup file type {item.file_type!r} "
                f"for logical file {item.logical_name!r}"
            )
        moves.append(
            f"MOVE {sql_string(item.logical_name)} "
            f"TO {sql_string(f'{container_dir}/{filename}')}"
        )
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
        eta: int | None
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


def try_set_multi_user(cfg: Config) -> None:
    """Return DB to MULTI_USER only when it exists and is not mid-restore."""
    state = database_state_desc(cfg)
    if state is None:
        logging.info("Skipping MULTI_USER cleanup: database does not exist")
        return
    progress = active_restore_progress(cfg)
    if state in RESTORE_IN_PROGRESS_STATES or progress is not None:
        logging.warning(
            "Skipping MULTI_USER cleanup: database state=%s active_restore=%s",
            state,
            progress is not None,
        )
        return
    database = sql_identifier(cfg.database)
    try:
        sqlcmd(
            cfg,
            f"ALTER DATABASE {database} SET MULTI_USER WITH ROLLBACK IMMEDIATE;",
            timeout=60,
        )
        logging.info("Database returned to MULTI_USER")
    except Exception:
        logging.exception("Could not return database to MULTI_USER")


def poll_until_online(
    cfg: Config,
    *,
    timeout_seconds: int | None = None,
    interval_seconds: int = POLL_INTERVAL_SECONDS,
) -> None:
    """Wait until the database is ONLINE; never issues RESTORE DATABASE."""
    budget = (
        cfg.poll_timeout_seconds if timeout_seconds is None else timeout_seconds
    )
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
                        f"{STUCK_RESTORING_SECONDS // 60} minutes with no active "
                        "RESTORE request; refusing to restart WITH REPLACE"
                    )
            else:
                stuck_since = None
            time.sleep(interval_seconds)
            continue

        if state is None:
            stuck_since = None
            time.sleep(interval_seconds)
            continue

        raise RuntimeError(
            f"Database {cfg.database} entered unexpected state {state!r} "
            "while waiting for restore to finish"
        )

    raise TimeoutError(
        f"Timed out after {budget / 3600:.1f} h waiting for "
        f"{cfg.database} to become ONLINE"
    )


def finish_restored_database(cfg: Config, backup_name: str) -> list[BackupFile]:
    """Verify ONLINE after restore; transaction logs remain on C: permanently."""
    files = read_backup_files(cfg, backup_name)
    if not database_is_online(cfg):
        raise RuntimeError(f"Database {cfg.database} is not ONLINE after restore")
    logging.info(
        "Database %s restored and ONLINE (data on D:, transaction logs on C:)",
        cfg.database,
    )
    return files


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
            f"available {data_free / 1024**3:.1f} GiB (including REPLACE reuse)"
        )
    if log_free < log_required:
        raise RuntimeError(
            "Not enough C: space for restored transaction log files; "
            f"required approximately {log_required / 1024**3:.1f} GiB, "
            f"available {log_free / 1024**3:.1f} GiB (including REPLACE reuse). "
            "Logs stay permanently on C: (expect ~174 GiB+ for this database)."
        )


def build_restore_sql(cfg: Config, backup_name: str, files: Iterable[BackupFile]) -> str:
    database = sql_identifier(cfg.database)
    backup_path = f"{CONTAINER_BACKUP_DIR}/{backup_name}"
    moves = restore_moves(cfg.database, files)
    return (
        "SET NOCOUNT ON; "
        f"IF DB_ID({sql_string(cfg.database)}) IS NOT NULL "
        f"ALTER DATABASE {database} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
        f"RESTORE DATABASE {database} FROM DISK={sql_string(backup_path)} "
        f"WITH REPLACE, RECOVERY, STATS=5, {', '.join(moves)}; "
        f"ALTER DATABASE {database} SET MULTI_USER;"
    )


def launch_restore_sql(cfg: Config, restore_sql: str) -> None:
    """Start RESTORE in a way that client disconnect does not abort monitoring.

    Prefers detached docker exec so Python/sqlcmd timeouts cannot tear down the
    client session mid-RESTORE. Falls back to attached sqlcmd; on TimeoutExpired
    switches to poll-until-ONLINE when the server-side restore is still healthy.
    Never issues a second WITH REPLACE while RESTORING.
    """
    if restore_in_progress(cfg):
        logging.info(
            "RESTORE already running for %s; will not issue WITH REPLACE",
            cfg.database,
        )
        return

    logging.info(
        "Launching detached RESTORE for %s (survives client disconnect)",
        cfg.database,
    )
    try:
        sqlcmd_detached(cfg, restore_sql)
    except Exception:
        logging.exception("Detached RESTORE launch failed; trying attached sqlcmd")
    else:
        grace_deadline = time.monotonic() + RESTORE_START_GRACE_SECONDS
        while time.monotonic() < grace_deadline:
            if database_is_online(cfg) or restore_in_progress(cfg):
                return
            time.sleep(5)
        if database_is_online(cfg) or restore_in_progress(cfg):
            return
        logging.warning(
            "Detached RESTORE did not become visible within %ss; "
            "falling back to attached sqlcmd",
            RESTORE_START_GRACE_SECONDS,
        )

    if restore_in_progress(cfg) or database_is_online(cfg):
        return

    logging.info(
        "Running attached RESTORE for %s (client timeout %ss)",
        cfg.database,
        cfg.restore_timeout_seconds,
    )
    try:
        output = sqlcmd(
            cfg,
            restore_sql,
            timeout=cfg.restore_timeout_seconds,
        )
        if output.strip():
            logging.info("SQL restore output:\n%s", output.strip())
    except subprocess.TimeoutExpired:
        if restore_in_progress(cfg) or database_is_online(cfg):
            logging.warning(
                "sqlcmd client timed out, but restore is still in progress "
                "or database is already ONLINE; switching to poll-until-ONLINE "
                "(will NOT restart WITH REPLACE)"
            )
            return
        try_set_multi_user(cfg)
        raise RuntimeError(
            "RESTORE sqlcmd timed out and no server-side restore is running"
        )
    except Exception:
        if restore_in_progress(cfg):
            logging.warning(
                "RESTORE client failed, but server-side restore is still running; "
                "switching to poll-until-ONLINE"
            )
            return
        try_set_multi_user(cfg)
        raise


def restore_database(cfg: Config, backup_name: str) -> None:
    if restore_in_progress(cfg):
        logging.info(
            "Database %s is already restoring; polling instead of WITH REPLACE",
            cfg.database,
        )
        poll_until_online(cfg)
        finish_restored_database(cfg, backup_name)
        return

    files = read_backup_files(cfg, backup_name)
    ensure_restore_space(cfg, files)
    restore_sql = build_restore_sql(cfg, backup_name, files)
    logging.info(
        "Restoring database %s; this can take many hours",
        cfg.database,
    )
    launch_restore_sql(cfg, restore_sql)
    if not database_is_online(cfg):
        poll_until_online(cfg)
    finish_restored_database(cfg, backup_name)


def save_state(path: Path, source: dict[str, object]) -> None:
    state = {
        **source,
        "database": source.get("database"),
        "restored_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def cleanup_old_backups(incoming_dir: Path, keep: Path) -> None:
    for path in incoming_dir.glob("*.bak"):
        if path.resolve() == keep.resolve():
            continue
        try:
            path.unlink()
            logging.info("Deleted old local backup: %s", path)
        except OSError:
            logging.exception("Could not delete old local backup: %s", path)


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
    """Exclusively hold the source .bak so peers cannot delete/rename it.

    Windows: CreateFileW with GENERIC_READ and FILE_SHARE_READ only (no
    FILE_SHARE_WRITE / FILE_SHARE_DELETE). SQL Server can still read the file
    over SMB/CIFS; other users cannot delete it while the handle is open.
    The lock is released on success or failure when the context exits.
    """

    _GENERIC_READ = 0x80000000
    _FILE_SHARE_READ = 0x00000001
    _OPEN_EXISTING = 3
    _FILE_ATTRIBUTE_NORMAL = 0x80
    _INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value if os.name == "nt" else -1

    def __init__(self, path: Path):
        self.path = path
        self._handle: int | object | None = None
        self.method = ""

    def __enter__(self) -> "SourceBackupLock":
        if os.name == "nt":
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
                    f"Cannot exclusively lock source backup {self.path}: "
                    f"CreateFileW failed (WinError {error})"
                )
            self._handle = int(handle)
            self.method = (
                "CreateFileW GENERIC_READ + FILE_SHARE_READ "
                "(no FILE_SHARE_WRITE/DELETE)"
            )
            logging.info(
                "Source bak locked: %s via %s",
                self.path,
                self.method,
            )
            return self

        # Non-Windows fallback: keep a read handle open for the restore duration.
        self._handle = self.path.open("rb")
        self.method = "open(rb) held for restore duration"
        logging.info("Source bak locked: %s via %s", self.path, self.method)
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
            logging.info("Source bak lock released: %s", self.path)
        except Exception:
            logging.exception("Failed to release source bak lock: %s", self.path)
        finally:
            self._handle = None


def restore_is_disabled(cfg: Config) -> Path | None:
    """Honor an explicit kill-switch so restore cannot be restarted by accident."""
    candidates = [
        Path(r"D:\mssql\RESTORE_DISABLED"),
        cfg.state_file.parent / "RESTORE_DISABLED",
        SCRIPT_DIR / "RESTORE_DISABLED",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def main() -> int:
    values = load_env(ENV_FILE)
    cfg = Config.from_env(values)
    ensure_directories(cfg)
    setup_logging(cfg.log_dir)

    disabled = restore_is_disabled(cfg)
    if disabled is not None or values.get("MSSQL_RESTORE_DISABLED", "").strip() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        marker = disabled or "MSSQL_RESTORE_DISABLED"
        logging.error("Restore is disabled (%s). Refusing to start.", marker)
        print(f"Restore is disabled ({marker}). Remove the flag to allow restore.")
        return 2

    lock_path = cfg.state_file.with_name("restore.lock")
    with ProcessLock(lock_path):
        connect_share(cfg)
        source = latest_backup(cfg.smb_share)
        ensure_source_is_complete(source, cfg.min_age_minutes)
        current = fingerprint(source)
        current["database"] = cfg.database
        logging.info(
            "Newest backup: %s, %.2f GiB, modified %s",
            source.name,
            int(current["bak_size"]) / 1024**3,
            current["bak_mtime"],
        )

        # Hold the source .bak for the entire restore (or early skip path)
        # so other users cannot delete it from the SMB share / Z: mapping.
        with SourceBackupLock(source):
            state = load_state(cfg.state_file)
            start_sql_server(cfg)
            wait_for_sql(cfg)
            if same_backup(state, current) and database_is_online(cfg):
                logging.info(
                    "Backup %s was already restored at %s; nothing to do",
                    source.name,
                    state.get("restored_at", "unknown time"),
                )
                return 0

            # Restore via Docker SMB/CIFS volume at /var/opt/mssql/backup.
            # Do not copy the .bak to D: — a ~600 GiB backup plus restored DB will not fit.
            bak_size = int(current["bak_size"])
            if restore_in_progress(cfg):
                logging.info(
                    "In-progress restore detected for %s (state=%s); "
                    "will poll until ONLINE and finish without WITH REPLACE "
                    "(source bak %s, %.2f GiB)",
                    cfg.database,
                    database_state_desc(cfg) or "UNKNOWN",
                    source.name,
                    bak_size / 1024**3,
                )
            else:
                logging.info(
                    "Restoring directly from container path %s/%s (%.2f GiB); "
                    "skipping local copy to preserve D: space for database files",
                    CONTAINER_BACKUP_DIR,
                    source.name,
                    bak_size / 1024**3,
                )
            restore_database(cfg, source.name)
            save_state(cfg.state_file, current)
            logging.info("Daily restore completed successfully")
            return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        logging.exception("Daily restore failed")
        raise SystemExit(1)
