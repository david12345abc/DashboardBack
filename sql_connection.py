"""
Подключение к SQL Server (erp_pm) на ii1 (192.168.1.157:1433).

Windows Auth от учётки TURBO-DON\\testii через LogonUser + impersonation
(текущий процесс — a.komarkova, поэтому Trusted_Connection «как есть» не подходит).

Hostname ``ii1``, не сырой IP: Trusted Connection по IP даёт 18452.
У Driver 17 нельзя писать Encrypt=no — драйвер ругается на атрибут
и игнорирует Connection Timeout (падает в 15 с).
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generator, Iterator

import pyodbc
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")
load_dotenv()

DEFAULT_SERVER = "ii1"
DEFAULT_DATABASE = "erp_pm"
DEFAULT_USER = r"TURBO-DON\testii"
DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"


def _parse_domain_user(user: str) -> tuple[str, str]:
    if "\\" in user:
        domain, username = user.split("\\", 1)
        return domain, username
    if "@" in user:
        username, domain = user.split("@", 1)
        return domain, username
    return "", user


@contextmanager
def windows_impersonation(user: str, password: str) -> Generator[None, None, None]:
    import win32con
    import win32security

    domain, username = _parse_domain_user(user)
    # NEW_CREDENTIALS ≈ runas /netonly — нужно для доступа к удалённому SQL под другой учёткой.
    logon_types = (
        win32con.LOGON32_LOGON_NEW_CREDENTIALS,
        win32con.LOGON32_LOGON_NETWORK,
        win32con.LOGON32_LOGON_INTERACTIVE,
    )

    last_error: Exception | None = None
    token = None
    for logon_type in logon_types:
        try:
            token = win32security.LogonUser(
                username,
                domain or None,
                password,
                logon_type,
                win32con.LOGON32_PROVIDER_DEFAULT,
            )
            break
        except Exception as exc:  # noqa: BLE001 - пробуем следующий тип логона
            last_error = exc
            token = None

    if token is None:
        raise RuntimeError(f"LogonUser failed for {user!r}: {last_error}")

    win32security.ImpersonateLoggedOnUser(token)
    try:
        yield
    finally:
        win32security.RevertToSelf()
        token.Close()


@dataclass
class SqlConnection:
    server: str = field(default_factory=lambda: os.getenv("SQL_SERVER", DEFAULT_SERVER))
    database: str = field(default_factory=lambda: os.getenv("SQL_DATABASE", DEFAULT_DATABASE))
    user: str = field(default_factory=lambda: os.getenv("SQL_USER", DEFAULT_USER))
    password: str = field(default_factory=lambda: os.getenv("SQL_PASSWORD", ""))
    driver: str = field(default_factory=lambda: os.getenv("SQL_DRIVER", DEFAULT_DRIVER))
    timeout: int = 45

    def _login_timeout(self) -> int:
        try:
            return max(15, int(self.timeout or 45))
        except (TypeError, ValueError):
            return 45

    def _resolve_driver(self) -> str:
        configured = (self.driver or "").strip()
        available = {name.casefold(): name for name in pyodbc.drivers()}
        if configured and configured.casefold() in available:
            return available[configured.casefold()]
        for candidate in (
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server",
        ):
            key = candidate.casefold()
            if key in available:
                return available[key]
        return configured or "SQL Server"

    def connection_string(self) -> str:
        driver = self._resolve_driver()
        timeout = self._login_timeout()
        parts = [
            f"DRIVER={{{driver}}}",
            f"SERVER={self.server}",
            f"DATABASE={self.database}",
            f"Connection Timeout={timeout}",
            "Trusted_Connection=yes",
        ]
        # Driver 17 отвергает Encrypt=no и тогда игнорирует Timeout.
        # Driver 18 по умолчанию Encrypt=yes — его надо задать явно.
        if "odbc driver 18" in driver.casefold():
            parts.append("Encrypt=no")
            parts.append("TrustServerCertificate=yes")
        return ";".join(parts) + ";"

    def connect(self) -> pyodbc.Connection:
        if not self.password:
            raise RuntimeError("SQL_PASSWORD не задан в .env")
        timeout = self._login_timeout()
        with windows_impersonation(self.user, self.password):
            return pyodbc.connect(self.connection_string(), timeout=timeout)

    @contextmanager
    def connect_ctx(self) -> Iterator[pyodbc.Connection]:
        if not self.password:
            raise RuntimeError("SQL_PASSWORD не задан в .env")
        timeout = self._login_timeout()
        with windows_impersonation(self.user, self.password):
            conn = pyodbc.connect(self.connection_string(), timeout=timeout)
            try:
                yield conn
            finally:
                conn.close()

    def ping(self) -> dict[str, Any]:
        with self.connect_ctx() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT @@SERVERNAME AS server_name, DB_NAME() AS db_name, "
                "SUSER_SNAME() AS login_name, ORIGINAL_LOGIN() AS original_login, "
                "CAST(@@VERSION AS nvarchar(256)) AS version"
            )
            row = cur.fetchone()
            info = dict(zip([c[0] for c in cur.description], row))
            cur.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            info["tables"] = cur.fetchone()[0]
            return info


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    sql = SqlConnection()
    print(f"Connecting to {sql.server} / {sql.database} as {sql.user} …")
    try:
        info = sql.ping()
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: {exc}")
        return 1

    version = str(info.pop("version", "")).split("\n")[0]
    for key, value in info.items():
        print(f"{key}: {value}")
    print(f"version: {version}")
    print("\nOK: SQL Server connection established.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
