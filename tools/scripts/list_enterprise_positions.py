"""
Должности и ФИО сотрудников из 1С (кадровая история + структура предприятия).

Обёртка над getkpi.list_enterprise_positions.

Примеры:
    python -m tools.scripts.list_enterprise_positions
    python -m tools.scripts.list_enterprise_positions "Технический директор"
    python -m tools.scripts.list_enterprise_positions --position "Технический директор"
    python -m tools.scripts.list_enterprise_positions --dept-path "Служба качества"
    python -m tools.scripts.list_enterprise_positions --employees-only --position "ГСПП"
    python -m tools.scripts.list_enterprise_positions --force --output report.txt
    py manage.py list_enterprise_positions --position "Технический директор"

Переменные окружения: ONEC_BASE_URL, ODATA_USER, ODATA_PASSWORD
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import requests

from getkpi.list_enterprise_positions import (
    AUTH,
    _department_matches,
    build_report,
    format_report,
    get_cached_report,
    normalize_text,
)


def _configure_stdout() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass


def _filter_rows(
    rows: list[dict],
    *,
    position: str | None,
    dept_path: str | None,
) -> list[dict]:
    filtered = rows
    if position:
        pos_norm = normalize_text(position)
        filtered = [
            row for row in filtered
            if normalize_text(row.get("position")) == pos_norm
        ]
    if dept_path:
        filtered = [
            row for row in filtered
            if _department_matches(row.get("department", ""), dept_path)
        ]
    return filtered


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Должности и ФИО сотрудников из 1С",
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=None,
        help="Корень в структуре предприятия (как positional-аргумент)",
    )
    parser.add_argument(
        "--root",
        dest="root_flag",
        default=None,
        help="То же, что positional: фильтр по ветке структуры предприятия",
    )
    parser.add_argument(
        "--position",
        default=None,
        help="Фильтр по названию должности",
    )
    parser.add_argument(
        "--dept-path",
        default=None,
        help="Фильтр по пути подразделения (совпадение или суффикс пути)",
    )
    parser.add_argument(
        "--employees-only",
        action="store_true",
        help="Вывести только список ФИО (по одному на строку)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Обновить данные из 1С, игнорируя дневной кэш",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Сохранить результат в файл (UTF-8 with BOM для .txt)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Вывести JSON-массив назначений",
    )
    args = parser.parse_args(argv)

    root_name = (args.root_flag or args.root or "").strip() or None
    session = requests.Session()
    session.auth = AUTH

    try:
        if root_name:
            rows = build_report(session, root_name)
        elif args.force:
            rows = get_cached_report(session=session, force=True)
        else:
            rows = get_cached_report(session=session)

        rows = _filter_rows(
            rows,
            position=args.position,
            dept_path=args.dept_path,
        )

        if args.employees_only:
            names = sorted(
                {
                    str(row.get("employee") or "").strip()
                    for row in rows
                    if str(row.get("employee") or "").strip()
                },
                key=normalize_text,
            )
            text = "\n".join(names)
        elif args.json:
            text = json.dumps(rows, ensure_ascii=False, indent=2)
        else:
            text = format_report(rows)

        if args.output:
            out_path = Path(args.output)
            encoding = "utf-8-sig" if out_path.suffix.lower() == ".txt" else "utf-8"
            suffix = "\n" if text and not text.endswith("\n") else ""
            out_path.write_text(text + suffix, encoding=encoding)
            print(f"Сохранено: {out_path.resolve()}")
        else:
            print(text, end="" if text.endswith("\n") else "\n")

        return 0
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1
    finally:
        session.close()


def main() -> None:
    _configure_stdout()
    raise SystemExit(run())


if __name__ == "__main__":
    main()
