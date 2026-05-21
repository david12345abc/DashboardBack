"""
Уровень внешнего брака по документам ТД_Форма0319 из 1С.

В 1С внешний брак оформляется документом Document_ТД_Форма0319
(источник несоответствия — «Внешний»). Структура та же, что у формы 0318.

Направления:
  - промышленное — ПодразделениеПоставщика = ОТК-1
  - бытовое — ПодразделениеПоставщика = ОТК-2

Примеры:
  python external_brak.py --month 2026-05
  python external_brak.py --from 2026-04-01 --to 2026-04-30
  python external_brak.py --csv external_brak.csv --month 2026-05
  python external_brak.py --table --month 2026-05
"""

from __future__ import annotations

import sys

from brak_report import ReportConfig, run_report

CONFIG = ReportConfig(
    doc_entity="Document_ТД_Форма0319",
    title="Внешний брак · ТД_Форма0319",
    description="Внешний брак по документам ТД_Форма0319.",
)


if __name__ == "__main__":
    try:
        run_report(CONFIG)
    except (LookupError, PermissionError, ValueError, RuntimeError) as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
