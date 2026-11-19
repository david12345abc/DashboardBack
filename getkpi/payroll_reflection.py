"""Непроведённое отражение зарплаты в бухучёте (_Document834).

Проводки регистра Хозрасчётный появляются только после проведения.
Пока документ месяца не проведён, факт ФОТ закрытого месяца читается из его строк:
оклад ``_Fld29420`` и взносы ``_Fld29432`` по подразделению ``_Fld29414RRef``.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Sequence

TBL_DOC = "_Document834"
TBL_VT = "_Document834_VT29410"
TBL_METHOD = "_Reference494"
TBL_ARTICLES = "_Chrc1945"

# 0x01 — строки, которые в расчёт ФОТ не входят.
# Для СУП за октябрь 2026 это ровно 10 963,91, без них факт совпадает со сводом.
COL_NOT_IN_FOT = "_Fld155321"


def _kopecks(value: Decimal | float | None) -> Decimal:
    amount = value if isinstance(value, Decimal) else Decimal(str(value or 0))
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def fetch_unposted_reflection(
    cur,
    period_start: str,
    period_end: str,
    dept_ids: Sequence[bytes],
    *,
    article_names: Sequence[str] | None = None,
    exclude_not_in_fot: bool = False,
) -> list[tuple[bytes, str, Decimal, Decimal]]:
    """Строки непроведённых отражений за период.

    Возвращает (подразделение, статья способа отражения, оклад, взносы).
    Пустой список — непроведённого документа по этим подразделениям нет.
    """
    if not dept_ids:
        return []

    article_sql = ""
    article_params: list[str] = []
    if article_names:
        article_sql = f" AND a._Description IN ({','.join('?' * len(article_names))})"
        article_params = list(article_names)

    skip_sql = ""
    if exclude_not_in_fot:
        skip_sql = f" AND (vt.{COL_NOT_IN_FOT} = 0x00 OR vt.{COL_NOT_IN_FOT} IS NULL)"

    cur.execute(
        f"""
        SELECT
            vt._Fld29414RRef AS dept_id,
            a._Description AS article,
            SUM(CAST(vt._Fld29420 AS decimal(18, 2))) AS salary,
            SUM(CAST(vt._Fld29432 AS decimal(18, 2))) AS insurance
        FROM dbo.{TBL_VT} vt WITH (NOLOCK)
        INNER JOIN dbo.{TBL_DOC} d WITH (NOLOCK)
            ON d._IDRRef = vt._Document834_IDRRef
        LEFT JOIN dbo.{TBL_METHOD} m WITH (NOLOCK)
            ON m._IDRRef = vt._Fld29416RRef
        LEFT JOIN dbo.{TBL_ARTICLES} a WITH (NOLOCK)
            ON a._IDRRef = m._Fld14261_RRRef
        WHERE d._Date_Time >= ? AND d._Date_Time < ?
          AND d._Fld29367 >= ? AND d._Fld29367 < ?
          AND d._Marked = 0x00
          AND d._Posted = 0x00
          AND vt._Fld29414RRef IN ({",".join("?" * len(dept_ids))})
          {article_sql}
          {skip_sql}
        GROUP BY vt._Fld29414RRef, a._Description
        """,
        [
            period_start,
            period_end,
            period_start,
            period_end,
            *dept_ids,
            *article_params,
        ],
    )
    out: list[tuple[bytes, str, Decimal, Decimal]] = []
    for dept_id, article, salary, insurance in cur.fetchall():
        out.append(
            (
                bytes(dept_id),
                article or "",
                _kopecks(salary),
                _kopecks(insurance),
            )
        )
    return out
