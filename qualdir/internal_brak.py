"""
Уровень внутреннего брака по документам ТД_Форма0318 из 1С.

Источник: Document_ТД_Форма0318 (источник несоответствия — «Внутренний»)
Направления:
  - промышленное — ПодразделениеПоставщика = ОТК-1
  - бытовое — ПодразделениеПоставщика = ОТК-2

Примеры:
  python internal_brak.py --month 2026-05
  python internal_brak.py --from 2026-04-01 --to 2026-04-30
  python internal_brak.py --csv internal_brak.csv --month 2026-05
  python internal_brak.py --table --month 2026-05
"""

from __future__ import annotations

import sys

from brak_report import ReportConfig, run_report

CONFIG = ReportConfig(
    doc_entity="Document_ТД_Форма0318",
    title="Внутренний брак · ТД_Форма0318",
    description="Внутренний брак по документам ТД_Форма0318.",
)


if __name__ == "__main__":
    try:
        run_report(CONFIG)
    except (LookupError, PermissionError, ValueError, RuntimeError) as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
