"""Принудительно пересчитать файловые кэши всех плиток дашборда.

1. Копия кэшей в backups/tiles_cache_<дата-время>/.
2. Удаление *.json / *.stamp из кэш-папок (скрипты расчёта ВП не трогаются).
3. Сборка дашборда каждого подразделения за текущий период и за месяцы 1..N
   текущего года + полный список задач cache_manager (таблицы, снимки ТурбоПроекта).
4. Файлы, которые не пересоздались (прошлые годы, разовые выгрузки), возвращаются из копии.

Запуск:  python tools/recache_all_tiles.py [--months-from 1] [--skip-months] [--dry-run]
"""
from __future__ import annotations

import argparse
import inspect
import logging
import os
import shutil
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Dashbord.settings")
django.setup()

from django.test import RequestFactory

from getkpi import cache_manager, chairman_data, views
from getkpi.kpi_periods import last_full_month

CACHE_DIRS = [ROOT / "getkpi" / "dashboard", ROOT / "dashboard"]
BACKUP_ROOT = ROOT / "backups"
PURGE_SUFFIXES = {".json", ".stamp"}
# Папки со скриптами расчёта ВП / факта, а не с кэшем плиток.
KEEP_DIRS = {"ВаловаяПрибыль", "факт"}
KEEP_FILES = {"enterprise_positions_cache.json"}

logger = logging.getLogger("recache_all_tiles")


def _say(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def _purge_candidates() -> list[Path]:
    out: list[Path] = []
    for base in CACHE_DIRS:
        if not base.exists():
            continue
        for p in base.rglob("*"):
            if not p.is_file() or p.suffix.lower() not in PURGE_SUFFIXES:
                continue
            rel_parts = p.relative_to(base).parts
            if rel_parts and rel_parts[0] in KEEP_DIRS:
                continue
            if p.name in KEEP_FILES:
                continue
            out.append(p)
    return out


def _backup(stamp: str) -> Path:
    dest = BACKUP_ROOT / f"tiles_cache_{stamp}"
    for base in CACHE_DIRS:
        if base.exists():
            shutil.copytree(base, dest / base.relative_to(ROOT), dirs_exist_ok=True)
    return dest


def _restore_missing(backup_dir: Path, purged: list[Path]) -> list[Path]:
    restored: list[Path] = []
    for p in purged:
        if p.exists():
            continue
        src = backup_dir / p.relative_to(ROOT)
        if src.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, p)
            restored.append(p)
    return restored


def _build_department_requests(months: list[int], year: int) -> list[tuple[str, dict]]:
    params_variants: list[dict] = [{}]
    params_variants += [{"year": str(year), "month": str(m)} for m in months]

    reqs: list[tuple[str, dict]] = []
    for dept in views._get_departments():
        for params in params_variants:
            if chairman_data.is_chairman_department(dept):
                for blk in chairman_data.CHAIRMAN_FOR_BLOCKS:
                    reqs.append((dept, {**params, "for": str(blk["id"])}))
            else:
                reqs.append((dept, dict(params)))
    return reqs


def _run_departments(months: list[int], year: int) -> list[str]:
    root_dept = next(iter(views.get_structure_data().keys()))
    all_depts = set(views._get_departments())
    # Скрипт работает «от имени» корня структуры и видит всё, включая подразделения вне structure.json.
    views._get_allowed_departments = lambda _user_dept: all_depts | {root_dept}

    raw_view = inspect.unwrap(views.get_all_departments)
    rf = RequestFactory()
    user = type("RecacheUser", (), {"department": root_dept})()

    failures: list[str] = []
    reqs = _build_department_requests(months, year)
    _say(f"Подразделений: {len(all_depts)}, запросов на сборку: {len(reqs)}")
    for i, (dept, params) in enumerate(reqs, 1):
        label = f"{dept} {params or '(текущий период)'}"
        request = rf.get("/api/kpi/all/", {"department": dept, **params})
        request.current_user = user
        t0 = time.monotonic()
        try:
            resp = raw_view(request)
            status = getattr(resp, "status_code", "?")
            ok = status == 200
        except Exception:
            logger.exception("Сборка упала: %s", label)
            status, ok = "exception", False
        dt = time.monotonic() - t0
        _say(f"  [{i}/{len(reqs)}] {'OK ' if ok else 'ERR'} {label} — {dt:.1f}s (HTTP {status})")
        if not ok:
            failures.append(label)
    return failures


def _run_warm_tasks(ref_y: int, ref_m: int) -> list[str]:
    tasks = cache_manager._build_warm_tasks(ref_y, ref_m)
    _say(f"Задач прогрева cache_manager: {len(tasks)}")
    failures: list[str] = []
    for i, (key, _path, fn) in enumerate(tasks, 1):
        t0 = time.monotonic()
        try:
            cache_manager.locked_call(key, fn)
            ok = True
        except Exception:
            logger.exception("Задача прогрева упала: %s", key)
            ok = False
        _say(f"  [{i}/{len(tasks)}] {'OK ' if ok else 'ERR'} {key} — {time.monotonic() - t0:.1f}s")
        if not ok:
            failures.append(key)
    return failures


def _wait_background_refresh(timeout_s: float = 1800) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        alive = [t for t in threading.enumerate() if t.name.startswith("cache-refresh-") and t.is_alive()]
        if not alive or time.monotonic() > deadline:
            if alive:
                _say(f"Не дождались фоновых пересчётов: {[t.name for t in alive]}")
            return
        _say(f"Ждём фоновые пересчёты: {len(alive)}")
        time.sleep(10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--months-from", type=int, default=1, help="с какого месяца текущего года пересобирать")
    parser.add_argument("--skip-months", action="store_true", help="только текущий период, без прошлых месяцев")
    parser.add_argument("--dry-run", action="store_true", help="показать, что будет удалено, и выйти")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    today = date.today()
    lfm_y, lfm_m = last_full_month(today)
    months = [] if args.skip_months else list(range(max(1, args.months_from), lfm_m + 1)) if lfm_y == today.year else []

    purged = _purge_candidates()
    _say(f"Файлов кэша к пересчёту: {len(purged)}; месяцы {today.year}: {months or 'только текущий период'}")
    if args.dry_run:
        for p in purged:
            print("  ", p.relative_to(ROOT))
        return

    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    backup_dir = _backup(stamp)
    _say(f"Резервная копия: {backup_dir}")

    for p in purged:
        p.unlink(missing_ok=True)
    _say(f"Удалено файлов кэша: {len(purged)}")

    t0 = time.monotonic()
    dept_failures = _run_departments(months, today.year)
    warm_failures = _run_warm_tasks(today.year, today.month)
    _wait_background_refresh()

    restored = _restore_missing(backup_dir, purged)
    regenerated = sum(1 for p in purged if p.exists()) - len(restored)

    _say("=" * 60)
    _say(f"Готово за {(time.monotonic() - t0) / 60:.1f} мин")
    _say(f"Пересчитано заново: {regenerated} из {len(purged)} файлов")
    _say(f"Возвращено из копии (не пересоздавались): {len(restored)}")
    for p in restored:
        print("   ", p.relative_to(ROOT))
    if dept_failures or warm_failures:
        _say(f"Ошибки сборки подразделений: {len(dept_failures)}")
        for f in dept_failures:
            print("   ", f)
        _say(f"Ошибки задач прогрева: {len(warm_failures)}")
        for f in warm_failures:
            print("   ", f)
    _say(f"Резервная копия: {backup_dir}")


if __name__ == "__main__":
    main()
