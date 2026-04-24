"""
calc_psd_portfolio.py — Портфель проектов ПСД по месяцам.

Что считает:
  • portfolio_count  — число проектов из TurboProject, у которых:
      - `has_1c == true`,
      - есть хотя бы одна веха (`is_milestone=true`) с датой окончания в месяце.
  • deviation_count  — из portfolio_count проекты со сдвигом по baseline
                       на уровне проекта: finish_date > baseline_finish.

Публичный API:
  • get_psd_portfolio_snapshot(month_arg)
  • get_psd_portfolio_monthly(year, ref_month)

Агрегация на фронте:
  в `plan` кладём portfolio_count,
  в `fact` кладём deviation_count,
  процент = fact / plan × 100.
"""

import functools
import json
import sys
from calendar import monthrange
from datetime import date, datetime
from typing import Any
from pathlib import Path

import requests

from .turboproject_config import API_BASE, EMAIL, PASSWORD, TIMEOUT

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "psd_portfolio_1c_milestones_v2"


# ═══════════════════════════════════════════════════════
# HTTP
# ═══════════════════════════════════════════════════════

def login(session: requests.Session | None = None) -> str:
    http = session or requests.Session()
    resp = http.post(
        f"{API_BASE}/api/auth/login",
        json={"email": EMAIL, "password": PASSWORD},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def api_get(
    path: str,
    token: str,
    params: dict[str, Any] | None = None,
    *,
    session: requests.Session | None = None,
) -> Any:
    http = session or requests.Session()
    resp = http.get(
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


# ═══════════════════════════════════════════════════════
# РАЗБОР ДАТ
# ═══════════════════════════════════════════════════════

def parse_iso_date(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def is_real_date(dt: datetime | None) -> bool:
    """1С часто отдаёт 0001-01-01 — это «пусто»."""
    return dt is not None and dt.year > 1900


# ═══════════════════════════════════════════════════════
# ЛОГИКА МЕСЯЦА
# ═══════════════════════════════════════════════════════

def parse_month_arg(value: str) -> tuple[date, date]:
    """'2026-04' -> (2026-04-01, 2026-04-30)."""
    y, m = value.split("-")
    y_i, m_i = int(y), int(m)
    start = date(y_i, m_i, 1)
    end = date(y_i, m_i, monthrange(y_i, m_i)[1])
    return start, end


def _cache_path_snapshot(month_arg: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"psd_portfolio_{month_arg}.json"


def _cache_path_monthly(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"psd_portfolio_monthly_{year}_{ref_month:02d}.json"


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _save_json(path: Path, data: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def has_milestone_in_month(tasks: list[dict[str, Any]], m_start: date, m_end: date) -> bool:
    """Есть ли milestone-задача с finish_date в [m_start; m_end]."""
    for t in tasks:
        if t.get("is_summary"):
            continue
        if "is_milestone" in t and not t.get("is_milestone"):
            continue
        fin = parse_iso_date(t.get("finish_date"))
        if not is_real_date(fin):
            continue
        d = fin.date()
        if m_start <= d <= m_end:
            return True
    return False


def project_slips_baseline(project_meta: dict[str, Any]) -> bool:
    """Сдвиг по baseline: dates.finish_date > dates.baseline_finish."""
    fin = parse_iso_date(project_meta.get("finish_date"))
    base = parse_iso_date(project_meta.get("baseline_finish"))
    if not (is_real_date(fin) and is_real_date(base)):
        return False
    return fin.date() > base.date()


def _month_end(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def _build_project_info(
    item: dict[str, Any],
    project_meta: dict[str, Any],
) -> dict[str, Any]:
    return {
        "file_id": item.get("id"),
        "project_name": project_meta.get("name") or item.get("original_name"),
        "start_date": project_meta.get("start_date"),
        "finish_date": project_meta.get("finish_date"),
        "baseline_finish": project_meta.get("baseline_finish"),
        "has_1c": bool(item.get("has_1c")),
    }


def _fetch_all_project_details(session: requests.Session, token: str) -> list[dict[str, Any]]:
    summary = api_get("/api/projects/files", token, session=session)
    items = summary.get("items") or []

    projects: list[dict[str, Any]] = []
    for item in items:
        if not item.get("id") or not item.get("has_1c"):
            continue
        try:
            details = api_get(f"/api/projects/files/{item['id']}", token, session=session)
        except requests.HTTPError:
            continue
        projects.append({
            "summary": item,
            "project": details.get("project") or {},
            "tasks": details.get("tasks") or [],
        })
    return projects


def _calc_month_payload(
    month_arg: str,
    projects: list[dict[str, Any]],
    *,
    include_details: bool = True,
) -> dict[str, Any]:
    m_start, m_end = parse_month_arg(month_arg)
    portfolio: list[dict[str, Any]] = []
    with_deviation: list[dict[str, Any]] = []

    for item in projects:
        summary = item.get("summary") or {}
        project_meta = item.get("project") or {}
        tasks = item.get("tasks") or []

        if not has_milestone_in_month(tasks, m_start, m_end):
            continue

        info = _build_project_info(summary, project_meta)
        portfolio.append(info)

        if project_slips_baseline(project_meta):
            fin = parse_iso_date(project_meta.get("finish_date"))
            base = parse_iso_date(project_meta.get("baseline_finish"))
            info_dev = dict(info)
            info_dev["slip_days"] = (
                (fin.date() - base.date()).days if fin and base else None
            )
            with_deviation.append(info_dev)

    payload = {
        "month": month_arg,
        "period_from": m_start.isoformat(),
        "period_to": m_end.isoformat(),
        "generated": datetime.now().isoformat(timespec="seconds"),
        "source": SOURCE_TAG,
        "portfolio_count": len(portfolio),
        "deviation_count": len(with_deviation),
    }
    if include_details:
        payload["portfolio"] = portfolio
        payload["deviation"] = with_deviation
    return payload


def get_psd_portfolio_snapshot(month_arg: str) -> dict:
    cache_path = _cache_path_snapshot(month_arg)
    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        return cached

    session = requests.Session()
    token = login(session)
    projects = _fetch_all_project_details(session, token)
    payload = _calc_month_payload(month_arg, projects, include_details=True)
    _save_json(cache_path, payload)
    return payload


def get_psd_portfolio_monthly(year: int, ref_month: int) -> dict:
    cache_path = _cache_path_monthly(year, ref_month)
    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        rows = cached.get("months") or []
        if rows and all(
            "portfolio_count" in row and "deviation_count" in row for row in rows
        ):
            return cached

    today = date.today()
    months: list[tuple[int, str]] = []
    for mm in range(1, ref_month + 1):
        month_end = _month_end(year, mm)
        if year > today.year or (year == today.year and mm > today.month):
            break
        months.append((mm, f"{year}-{mm:02d}"))

    if not months:
        payload = {
            "year": year,
            "ref_month": ref_month,
            "source": SOURCE_TAG,
            "months": [],
        }
        _save_json(cache_path, payload)
        return payload

    session = requests.Session()
    token = login(session)
    projects = _fetch_all_project_details(session, token)

    rows_out: list[dict[str, Any]] = []
    for mm, month_arg in months:
        month_payload = _calc_month_payload(month_arg, projects, include_details=False)
        rows_out.append({
            "year": year,
            "month": mm,
            "period_from": month_payload["period_from"],
            "period_to": month_payload["period_to"],
            "portfolio_count": month_payload["portfolio_count"],
            "deviation_count": month_payload["deviation_count"],
        })
        snapshot = _calc_month_payload(month_arg, projects, include_details=True)
        _save_json(_cache_path_snapshot(month_arg), snapshot)

    payload = {
        "year": year,
        "ref_month": ref_month,
        "source": SOURCE_TAG,
        "months": rows_out,
    }
    _save_json(cache_path, payload)
    return payload


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}

    if args:
        month_arg = args[0]
    else:
        today = date.today()
        month_arg = f"{today.year}-{today.month:02d}"

    m_start, m_end = parse_month_arg(month_arg)
    save_json = "--json" in flags
    show_details = "--details" in flags

    print("=" * 78)
    print(f"  ПСД · Портфель проектов за {month_arg}")
    print(f"  Период:  {m_start} — {m_end}")
    print("=" * 78)

    payload = get_psd_portfolio_snapshot(month_arg)
    portfolio = payload.get("portfolio") or []
    with_deviation = payload.get("deviation") or []

    # ─── Вывод ───
    print("\n" + "=" * 78)
    print(f"  РЕЗУЛЬТАТ · {month_arg}")
    print("=" * 78)
    print(f"  Портфель (план) — проектов с вехами в месяце:  {len(portfolio):>4d}")
    print(f"  Отклонения по baseline (сдвиг):                 {len(with_deviation):>4d}")

    if show_details and portfolio:
        print(f"\n  Портфель:")
        for p in portfolio:
            tag = "  ⚠ сдвиг" if any(d["file_id"] == p["file_id"] for d in with_deviation) else ""
            print(f"    • [{p['file_id']}] {p['project_name']}{tag}")
            print(f"        finish={p.get('finish_date')}  baseline={p.get('baseline_finish')}")

    # ─── JSON ───
    if save_json:
        out_path = _cache_path_snapshot(month_arg)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"\n  → {out_path}")


if __name__ == "__main__":
    main()
