"""CRM BP by Ответственный Vor May; also HeadTask links."""
from __future__ import annotations

import functools
import json
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

VOR = "a312e193-d7b7-11ee-94a5-6cb31113810e"
BP = "BusinessProcess_CRM_БизнесПроцесс"
VK870 = "05dc41cd-3fb7-11f1-97ed-6cb31113810e"


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    filters = [
        (
            f"Ответственный_Key eq guid'{VOR}' and year(Date) eq 2026 and month(Date) eq 5",
            "resp_may",
        ),
        (
            f"Автор_Key eq guid'{VOR}' and year(Date) eq 2026 and month(Date) eq 5",
            "author_may",
        ),
        (
            f"year(ДатаСтарта) eq 2026 and month(ДатаСтарта) eq 5 and Ответственный_Key eq guid'{VOR}'",
            "start_may",
        ),
        (
            f"year(ПланируемаяДатаЗавершения) eq 2026 and month(ПланируемаяДатаЗавершения) eq 5 and Ответственный_Key eq guid'{VOR}'",
            "plan_may",
        ),
        (f"substringof('Входящая', Наименование) and year(Date) eq 2026", "name_vk_2026"),
        (f"substringof('002870', Наименование)", "num_in_name"),
        (f"substringof('002870', Описание)", "num_in_opis"),
        (f"substringof('002870', ВидОбъектаСтрокой)", "vid"),
        (f"substringof('ВходящаяКорреспонденция', ВидОбъектаСтрокой)", "vid_vk"),
    ]
    for filt, label in filters:
        url = f"{BASE}/{quote(BP)}?$filter={quote(filt)}&$top=30&$format=json"
        resp = s.get(url, timeout=90)
        print(f"{label}: {resp.status_code}", end=" ")
        if not resp.ok:
            print(resp.text[:200].replace("\n", " "))
            continue
        rows = resp.json().get("value") or []
        print(f"rows={len(rows)}")
        for r in rows[:8]:
            print(
                " ",
                r.get("Date"),
                r.get("ДатаСтарта"),
                r.get("ПланируемаяДатаЗавершения"),
                r.get("Состояние"),
                (r.get("Наименование") or "")[:80],
                r.get("Предмет"),
                r.get("Предмет_Type"),
                r.get("ВидОбъектаСтрокой"),
            )

    # Tasks where HeadTask / business process?
    # Check Task with Date around assignment and Description empty but subject?
    TASK = "Task_ЗадачаИсполнителя"
    filt = (
        f"year(Date) eq 2026 and month(Date) eq 5 "
        f"and substringof('Входящая корреспонденция НП00-003', ПредметСтрокой)"
    )
    url = f"{BASE}/{quote(TASK)}?$filter={quote(filt)}&$top=50&$format=json"
    resp = s.get(url, timeout=90)
    print(f"\nTask May VK NP00-003*: {resp.status_code}", end=" ")
    if resp.ok:
        rows = resp.json().get("value") or []
        print(f"rows={len(rows)}")
        for r in rows[:20]:
            print(
                r.get("Date"),
                r.get("СрокИсполнения"),
                r.get("Исполнитель"),
                r.get("Исполнитель_Type"),
                (r.get("ПредметСтрокой") or "")[:100],
            )


if __name__ == "__main__":
    main()
