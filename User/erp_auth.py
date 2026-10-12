"""Вход по пользователям 1С из erp_pm.

Таблицы те же, что в Constructor (backend/app/clients/erp_sql.py):
v8users — ФИО и пароль, _Reference366/_Reference513 — подразделение,
_Reference596/_InfoRg43757/_Reference164 — должность.
Соединение — то же, что у остального дашборда (sql_connection.SqlConnection).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import pyodbc

from sql_connection import SqlConnection

from .department_map import dashboard_leaders
from .onec_password import verify_password

logger = logging.getLogger(__name__)

_FIO_EXPR = "LTRIM(RTRIM(COALESCE(NULLIF(v.Descr, N''), v.Name)))"
_LIST_CACHE_TTL_SEC = 120.0
_list_cache: tuple[float, list[dict[str, str]]] | None = None

_DEPT_JOINS = """
    LEFT JOIN dbo._Reference366 u WITH (NOLOCK)
        ON LTRIM(RTRIM(u._Description)) = LTRIM(RTRIM(COALESCE(NULLIF(v.Descr, N''), v.Name)))
    LEFT JOIN dbo._Reference513 d1 WITH (NOLOCK) ON u._Fld10996RRef = d1._IDRRef
    LEFT JOIN dbo._Reference513 d2 WITH (NOLOCK) ON u._Fld74621RRef = d2._IDRRef
    LEFT JOIN dbo._Reference513 d3 WITH (NOLOCK) ON u._Fld166077RRef = d3._IDRRef
    LEFT JOIN dbo._Reference513 d4 WITH (NOLOCK) ON u._Fld166081RRef = d4._IDRRef
    LEFT JOIN dbo._Reference513 d5 WITH (NOLOCK) ON u._Fld166082RRef = d5._IDRRef
"""

_DEPT_NAME_EXPR = """
    CAST(
        COALESCE(
            NULLIF(LTRIM(RTRIM(d1._Description)), N''),
            NULLIF(LTRIM(RTRIM(d2._Description)), N''),
            NULLIF(LTRIM(RTRIM(d3._Description)), N''),
            NULLIF(LTRIM(RTRIM(d4._Description)), N''),
            NULLIF(LTRIM(RTRIM(d5._Description)), N''),
            N''
        ) AS nvarchar(256)
    )
"""

_POSITION_SQL = """
    SELECT TOP 1 CAST(pos._Description AS nvarchar(256)) AS Position
    FROM dbo._Reference596 person WITH (NOLOCK)
    INNER JOIN dbo._InfoRg43757 t WITH (NOLOCK) ON t._Fld43761RRef = person._IDRRef
    INNER JOIN dbo._Reference164 pos WITH (NOLOCK) ON t._Fld43766RRef = pos._IDRRef
    WHERE LTRIM(RTRIM(person._Description)) = ?
      AND LTRIM(RTRIM(pos._Description)) <> N''
      AND t._Fld43775 >= '5999-01-01'
    ORDER BY t._Fld43774 DESC
"""

_POSITION_FALLBACK_SQL = """
    SELECT TOP 1 CAST(pos._Description AS nvarchar(256)) AS Position
    FROM dbo._Reference596 person WITH (NOLOCK)
    INNER JOIN dbo._InfoRg43757 t WITH (NOLOCK) ON t._Fld43761RRef = person._IDRRef
    INNER JOIN dbo._Reference164 pos WITH (NOLOCK) ON t._Fld43766RRef = pos._IDRRef
    WHERE LTRIM(RTRIM(person._Description)) = ?
      AND LTRIM(RTRIM(pos._Description)) <> N''
    ORDER BY t._Fld43774 DESC
"""


class ErpAuthError(Exception):
    """erp_pm недоступна или запрос не выполнился."""


class UserNotFoundError(ErpAuthError):
    pass


class AmbiguousUserError(ErpAuthError):
    pass


class DashboardAccessError(ErpAuthError):
    """Пароль 1С верный, но человек не занимает руководящую должность подразделения."""


@dataclass(frozen=True)
class OnecUser:
    user_id: str
    fio: str
    department_1c: str
    position: str
    department: str


def _as_bytes(value) -> bytes:
    if value is None:
        return b""
    if isinstance(value, memoryview):
        return value.tobytes()
    return bytes(value)


def _connect():
    sql = SqlConnection()
    conn = sql.connect()
    try:
        conn.timeout = 60
    except (pyodbc.Error, OSError):
        pass
    return conn


def _position(cur, fio: str) -> str:
    cur.execute(_POSITION_SQL, (fio,))
    row = cur.fetchone()
    if row and (row.Position or "").strip():
        return str(row.Position).strip()
    cur.execute(_POSITION_FALLBACK_SQL, (fio,))
    row = cur.fetchone()
    if row and (row.Position or "").strip():
        return str(row.Position).strip()
    return ""


def list_login_users() -> list[dict[str, str]]:
    """Руководители подразделений из списка 1С: ФИО, должность и узел дашборда."""
    global _list_cache
    now = time.monotonic()
    if _list_cache is not None and now - _list_cache[0] < _LIST_CACHE_TTL_SEC:
        return list(_list_cache[1])

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cur.execute(
            f"""
            SELECT
                {_FIO_EXPR} AS Fio,
                {_DEPT_NAME_EXPR} AS Department,
                CAST(job.Position AS nvarchar(256)) AS Position
            FROM dbo.v8users v WITH (NOLOCK)
            {_DEPT_JOINS}
            OUTER APPLY (
                SELECT TOP 1 pos._Description AS Position
                FROM dbo._Reference596 person WITH (NOLOCK)
                INNER JOIN dbo._InfoRg43757 t WITH (NOLOCK)
                    ON t._Fld43761RRef = person._IDRRef
                INNER JOIN dbo._Reference164 pos WITH (NOLOCK)
                    ON t._Fld43766RRef = pos._IDRRef
                WHERE LTRIM(RTRIM(person._Description)) = {_FIO_EXPR}
                  AND LTRIM(RTRIM(pos._Description)) <> N''
                ORDER BY
                    CASE WHEN t._Fld43775 >= '5999-01-01' THEN 0 ELSE 1 END,
                    t._Fld43774 DESC
            ) job
            WHERE v.Show = 1
              AND {_FIO_EXPR} <> N''
            """
        )
        raw: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for row in cur.fetchall():
            fio = (row.Fio or "").strip()
            if not fio or fio.casefold() in seen:
                continue
            seen.add(fio.casefold())
            raw.append((fio, (row.Department or "").strip(), (row.Position or "").strip()))
        users = dashboard_leaders(raw)
    except pyodbc.Error as exc:
        raise ErpAuthError(f"Не удалось прочитать список пользователей 1С: {exc}") from exc
    finally:
        conn.close()

    _list_cache = (now, users)
    return list(users)


def merge_login_users(
    leaders: list[dict[str, str]],
    legacy: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Руководители 1С плюс уже существующие учётки дашборда.

    Если логин совпадает, остаются должность и дашборд руководителя.
    """
    by_key: dict[str, dict[str, str]] = {}
    for item in legacy:
        nickname = (item.get("nickname") or "").strip()
        if not nickname:
            continue
        by_key[nickname.casefold()] = {
            "nickname": nickname,
            "department": (item.get("department") or "").strip(),
            "position": (item.get("position") or "").strip(),
        }
    for item in leaders:
        nickname = (item.get("nickname") or "").strip()
        if not nickname:
            continue
        prev = by_key.get(nickname.casefold())
        by_key[nickname.casefold()] = {
            "nickname": nickname,
            "department": (item.get("department") or (prev or {}).get("department") or "").strip(),
            "position": (item.get("position") or (prev or {}).get("position") or "").strip(),
        }
    return sorted(by_key.values(), key=lambda row: row["nickname"].casefold())


def _leader_by_fio(fio: str) -> dict[str, str] | None:
    key = " ".join((fio or "").split()).casefold()
    if not key:
        return None
    for item in list_login_users():
        if item["nickname"].casefold() == key:
            return item
    return None


def authenticate(fio: str, password: str) -> OnecUser:
    """Найти пользователя по ФИО и проверить пароль 1С."""
    fio = " ".join((fio or "").split())
    if not fio or not password:
        raise UserNotFoundError("empty")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cur.execute(
            f"""
            SELECT
                CONVERT(varchar(64), v.ID, 2) AS UserId,
                CAST(v.Name AS nvarchar(128)) AS Name,
                CAST(v.Descr AS nvarchar(256)) AS Descr,
                v.Data AS Data,
                {_DEPT_NAME_EXPR} AS Department
            FROM dbo.v8users v WITH (NOLOCK)
            {_DEPT_JOINS}
            WHERE LTRIM(RTRIM(v.Name)) = ? OR LTRIM(RTRIM(v.Descr)) = ?
            """,
            (fio, fio),
        )
        by_id: dict[str, tuple] = {}
        for row in cur.fetchall():
            user_id = (row.UserId or "").strip().upper()
            if not user_id:
                continue
            prev = by_id.get(user_id)
            dept = (row.Department or "").strip()
            if prev is None or (dept and not (prev[4] or "").strip()):
                by_id[user_id] = row
        if not by_id:
            raise UserNotFoundError("not found")
        if len(by_id) > 1:
            raise AmbiguousUserError("ambiguous")

        row = next(iter(by_id.values()))
        data = _as_bytes(row.Data)
        if not verify_password(data, password):
            raise UserNotFoundError("bad password")

        descr = (row.Descr or "").strip()
        name = (row.Name or "").strip()
        canon = descr or name or fio
        department_1c = (row.Department or "").strip()
        position = _position(cur, canon)
        leader = _leader_by_fio(canon) or _leader_by_fio(fio)
        if leader is None:
            raise DashboardAccessError("not a head")
        return OnecUser(
            user_id=(row.UserId or "").strip().upper(),
            fio=canon,
            department_1c=department_1c,
            position=leader.get("position") or position,
            department=leader["department"],
        )
    except pyodbc.Error as exc:
        raise ErpAuthError(f"Ошибка запроса к erp_pm: {exc}") from exc
    finally:
        conn.close()
