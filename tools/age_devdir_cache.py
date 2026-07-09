"""Сделать devdir-кэш «устаревшим» для проверки stale-while-revalidate.

Меняет cache_date с сегодня на вчера, **не удаляя** файлы.
Удаление файлов ломает SWR: без файла идёт синхронный пересчёт.

Пример:
    python tools/age_devdir_cache.py
    python tools/age_devdir_cache.py --date 2026-07-08
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path


def _cache_dirs() -> list[Path]:
    root = Path(__file__).resolve().parent.parent
    return [
        root / "getkpi" / "dashboard",
        root / "dashboard",
    ]


def age_devdir_cache(*, target_date: str | None = None, dry_run: bool = False) -> list[str]:
    today = date.today().isoformat()
    new_date = target_date or (date.today() - timedelta(days=1)).isoformat()
    changed: list[str] = []

    for base in _cache_dirs():
        if not base.exists():
            continue
        for path in sorted(base.glob("*devdir*")):
            if not path.is_file() or path.suffix != ".json":
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if raw.get("cache_date") != today:
                continue
            raw["cache_date"] = new_date
            changed.append(path.name)
            if not dry_run:
                path.write_text(
                    json.dumps(raw, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        help="Новая cache_date (по умолчанию — вчера)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только показать, что будет изменено",
    )
    args = parser.parse_args()

    changed = age_devdir_cache(target_date=args.date, dry_run=args.dry_run)
    action = "Would change" if args.dry_run else "Changed"
    print(f"{action} {len(changed)} file(s):")
    for name in changed:
        print(f"  {name}")


if __name__ == "__main__":
    main()
