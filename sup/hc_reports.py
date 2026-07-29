"""Общие пути и чтение HR-отчётов ``HC_сводный_{year}_{Month}.xls[x]``."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import xlrd
from openpyxl import load_workbook

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
    base_name = f"HC_сводный_{year}_{title}"
    for suffix in (".xls", ".xlsx"):
        path = HC_REPORTS_DIR / f"{base_name}{suffix}"
        if path.exists():
            return path
    return HC_REPORTS_DIR / f"{base_name}.xls"


class _OpenpyxlSheetAdapter:
    """Минимальный xlrd-совместимый интерфейс листа."""

    def __init__(self, sheet: Any) -> None:
        self._sheet = sheet
        self.nrows = sheet.max_row

    def row_values(self, row_idx: int) -> list[Any]:
        return [
            self._sheet.cell(row=row_idx + 1, column=column).value
            for column in range(1, self._sheet.max_column + 1)
        ]

    def cell_value(self, row_idx: int, column_idx: int) -> Any:
        return self._sheet.cell(row=row_idx + 1, column=column_idx + 1).value


class _OpenpyxlBookAdapter:
    """Минимальный xlrd-совместимый интерфейс книги."""

    datemode = 0

    def __init__(self, book: Any) -> None:
        self._book = book

    def sheet_names(self) -> list[str]:
        return list(self._book.sheetnames)

    def sheet_by_name(self, name: str) -> _OpenpyxlSheetAdapter:
        return _OpenpyxlSheetAdapter(self._book[name])


def open_hc_workbook(path: Path) -> Any:
    """Открыть старый XLS через xlrd, а XLSX — через openpyxl."""
    if path.suffix.lower() == ".xlsx":
        return _OpenpyxlBookAdapter(
            load_workbook(path, read_only=True, data_only=True),
        )
    return xlrd.open_workbook(str(path))


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
