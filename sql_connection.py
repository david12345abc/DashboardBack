"""
Подключение к SQL Server (erp_pm) на 192.168.1.157:1433.

Windows Auth от учётки TURBO-DON\\testii через LogonUser + impersonation
(текущий процесс — a.komarkova, поэтому Trusted_Connection «как есть» не подходит).
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

DEFAULT_SERVER = "192.168.1.157,1433"
DEFAULT_DATABASE = "erp_pm"
DEFAULT_USER = r"TURBO-DON\testii"
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"


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
    timeout: int = 15

    def connection_string(self) -> str:
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
            "Encrypt=no;"
            f"Connection Timeout={self.timeout};"
        )

    def connect(self) -> pyodbc.Connection:
        if not self.password:
            raise RuntimeError("SQL_PASSWORD не задан в .env")
        # Impersonation держим на время connect+work через connect_ctx().
        # Здесь — короткий connect под impersonation.
        with windows_impersonation(self.user, self.password):
            return pyodbc.connect(self.connection_string())

    @contextmanager
    def connect_ctx(self) -> Iterator[pyodbc.Connection]:
        if not self.password:
            raise RuntimeError("SQL_PASSWORD не задан в .env")
        with windows_impersonation(self.user, self.password):
            conn = pyodbc.connect(self.connection_string())
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
