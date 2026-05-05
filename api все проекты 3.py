import json
from datetime import datetime
from typing import Any

import requests


API_BASE = "http://192.168.1.236:8000"
EMAIL = "sktb_razvitie6@turbo-don.ru"
PASSWORD = "Ruslandavletov28"
TIMEOUT = 60


def login() -> str:
    resp = requests.post(
        f"{API_BASE}/api/auth/login",
        json={"email": EMAIL, "password": PASSWORD},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def api_get(path: str, token: str, params: dict[str, Any] | None = None) -> Any:
    resp = requests.get(
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


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


def iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def build_overdue_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    today = datetime.now().date()
    overdue = []

    for task in tasks:
        if task.get("is_summary"):
            continue

        percent_complete = float(task.get("percent_complete") or 0.0)
        if percent_complete >= 1.0:
            continue

        finish_dt = parse_iso_date(task.get("finish_date"))
        if finish_dt is None:
            continue

        if finish_dt.date() < today:
            overdue.append(
                {
                    "id": task.get("id"),
                    "uid": task.get("uid"),
                    "name": task.get("name"),
                    "start_date": iso_or_none(task.get("start_date")),
                    "finish_date": iso_or_none(task.get("finish_date")),
                    "percent_complete": percent_complete,
                    "executors": [
                        a.get("resource_name")
                        for a in (task.get("assignments") or [])
                        if a.get("resource_name")
                    ],
                }
            )

    overdue.sort(key=lambda x: (x.get("finish_date") or "", x.get("name") or ""))
    return overdue


def build_overdue_milestones(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    today = datetime.now().date()
    overdue = []

    for task in tasks:
        if not task.get("is_milestone"):
            continue

        percent_complete = float(task.get("percent_complete") or 0.0)
        if percent_complete >= 1.0:
            continue

        finish_dt = parse_iso_date(task.get("finish_date"))
        if finish_dt is None:
            continue

        if finish_dt.date() < today:
            overdue.append(
                {
                    "id": task.get("id"),
                    "uid": task.get("uid"),
                    "name": task.get("name"),
                    "start_date": iso_or_none(task.get("start_date")),
                    "finish_date": iso_or_none(task.get("finish_date")),
                    "percent_complete": percent_complete,
                }
            )

    overdue.sort(key=lambda x: (x.get("finish_date") or "", x.get("name") or ""))
    return overdue


def build_project_payload(summary_item: dict[str, Any], details: dict[str, Any]) -> dict[str, Any]:
    project_meta = details.get("project") or {}
    tasks = details.get("tasks") or []
    overdue_tasks = build_overdue_tasks(tasks)
    overdue_milestones = build_overdue_milestones(tasks)

    non_summary_tasks = [t for t in tasks if not t.get("is_summary")]
    completed_tasks = [
        t for t in non_summary_tasks
        if float(t.get("percent_complete") or 0.0) >= 1.0
    ]

    return {
        "file_id": summary_item.get("id"),
        "original_name": summary_item.get("original_name"),
        "uploaded_at": iso_or_none(summary_item.get("uploaded_at")),
        "project_name": (project_meta or {}).get("name") or summary_item.get("original_name"),
        "dates": {
            "start_date": iso_or_none(project_meta.get("start_date")),
            "finish_date": iso_or_none(project_meta.get("finish_date")),
            "actual_finish_date": iso_or_none(project_meta.get("actual_finish_date")),
            "baseline_start": iso_or_none(project_meta.get("baseline_start")),
            "baseline_finish": iso_or_none(project_meta.get("baseline_finish")),
            "plan_finish_1c": iso_or_none(project_meta.get("plan_finish_1c")),
        },
        "task_stats": {
            "total_tasks": len(tasks),
            "non_summary_tasks": len(non_summary_tasks),
            "completed_tasks": len(completed_tasks),
            "overdue_tasks_count": len(overdue_tasks),
            "overdue_milestones_count": len(overdue_milestones),
        },
        "overdue_tasks": overdue_tasks,
        "overdue_milestones": overdue_milestones,
        "data_1c": details.get("data_1c"),
    }


def main() -> None:
    token = login()

    summary = api_get("/api/projects/files", token)
    items = summary.get("items") or []
    projects_with_1c = [item for item in items if item.get("has_1c")]

    result = {
        "total_projects": len(items),
        "projects_with_1c_count": len(projects_with_1c),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "projects": [],
    }

    for item in projects_with_1c:
        file_id = item.get("id")
        if not file_id:
            continue
        details = api_get(f"/api/projects/files/{file_id}", token)
        result["projects"].append(build_project_payload(item, details))

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
