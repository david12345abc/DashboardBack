"""List SQL databases + search constants/settings for COM IB connection."""
from __future__ import annotations

import functools
import sys

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)


def main() -> None:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT name, state_desc, recovery_model_desc
            FROM sys.databases
            ORDER BY name
            """
        )
        print("=== databases ===")
        for r in cur.fetchall():
            print(r[0], r[1], r[2])

        # Search short text columns in Const / CommonSettings / InfoRg for COM/сервер ИБ
        print("\n=== connection-ish strings ===")
        cur.execute(
            """
            SELECT t.name, c.name, c.max_length
            FROM sys.tables t
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE ty.name IN ('nvarchar', 'nchar', 'varchar')
              AND (
                t.name LIKE '_Const%'
                OR t.name LIKE '_CommonSettings%'
                OR t.name LIKE '_InfoRg%Настрой%'
                OR t.name = '_CommonSettings'
              )
            """
        )
        cols = cur.fetchall()
        print("cols", len(cols))
        phrases = (
            "%COM%",
            "%Интеграц%ИБ%",
            "%V83.COMConnector%",
            "%Srvr=%",
            "%документ%оборот%",
            "%docflow%",
            "%ДО%",
            "%erp_%",
            "%ИсполнениеДокументовЭДО%",
            "%ТД_COM%",
        )
        for tname, cname, _ in cols:
            for ph in phrases:
                try:
                    cur.execute(
                        f"""
                        SELECT TOP 3 [{cname}] FROM [{tname}] WITH (NOLOCK)
                        WHERE [{cname}] LIKE ?
                        """,
                        ph,
                    )
                    rows = cur.fetchall()
                except Exception:
                    continue
                for row in rows:
                    val = str(row[0] or "")
                    if not val.strip():
                        continue
                    # filter noise
                    low = val.lower()
                    if ph == "%ДО%" and "документо" not in low and "com" not in low:
                        continue
                    print(f"{tname}.{cname}: {val[:250]}")

        # Also scan Reference catalogs descriptions for COM integration
        cur.execute(
            """
            SELECT t.name FROM sys.tables t
            WHERE t.name LIKE '_Reference%'
              AND EXISTS (
                SELECT 1 FROM sys.columns c
                WHERE c.object_id = t.object_id AND c.name = '_Description'
              )
            """
        )
        refs = [r[0] for r in cur.fetchall()]
        print("\n=== catalog descriptions ===")
        for tname in refs:
            try:
                cur.execute(
                    f"""
                    SELECT TOP 5 _Description FROM [{tname}] WITH (NOLOCK)
                    WHERE _Description LIKE N'%COM%'
                       OR _Description LIKE N'%Интеграция%ИБ%'
                       OR _Description LIKE N'%Исполнение документов ЭДО%'
                       OR _Description LIKE N'%COMИнтеграция%'
                    """
                )
                for row in cur.fetchall():
                    print(tname, row[0])
            except Exception:
                pass


if __name__ == "__main__":
    main()
