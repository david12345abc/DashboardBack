"""Общие пути и имена файлов HR-отчётов ``HC_сводный_{year}_{Month}.xls``."""
from __future__ import annotations

from pathlib import Path

HC_REPORTS_DIR = Path(
    r"\\192.168.1.198\Files\16.Отдел персонала\Отдел\Отчеты",
)

HC_FILE_MONTH_TITLES: dict[int, str] = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}


def hc_report_path(year: int, month: int) -> Path:
    title = HC_FILE_MONTH_TITLES[month]
    return HC_REPORTS_DIR / f"HC_сводный_{year}_{title}.xls"


def reports_mtime_ns(ref_y: int, ref_m: int) -> int:
    latest = 0
    for month in range(1, ref_m + 1):
        path = hc_report_path(ref_y, month)
        try:
            if path.exists():
                latest = max(latest, path.stat().st_mtime_ns)
        except OSError:
            continue
    return latest
