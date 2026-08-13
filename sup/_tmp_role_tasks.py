"""Tasks assigned to HR head role (not personal user)."""
from __future__ import annotations

import functools
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

ROLE = "6345f269-528e-11f1-9805-6cb31113810e"  # Начальник службы управления персоналом
ROLE_KADR = "93e488d9-2f44-11f1-97d7-6cb31113810e"  # Кадровик
ROLE_DP = "2c240ffd-eadc-11eb-866d-ac1f6b05524d"  # Директор по персоналу
REF = "a312e193-d7b7-11ee-94a5-6cb31113810e"


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    for label, role in [
        ("Начальник СУП", ROLE),
        ("Кадровик", ROLE_KADR),
        ("Директор по персоналу", ROLE_DP),
    ]:
        filt = (
            f"РольИсполнителя_Key eq guid'{role}' and DeletionMark eq false "
            f"and Date ge datetime'2026-05-01T00:00:00' "
            f"and Date lt datetime'2026-06-01T00:00:00'"
        )
        url = (
            f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
            f"?$format=json&$filter={quote(filt, safe='')}"
            f"&$orderby={quote('Date asc')}&$top=100"
        )
        r = session.get(url, timeout=120)
        print(f"\n=== {label} Date May: HTTP {r.status_code} ===")
        if not r.ok:
            print(r.text[:250])
            continue
        rows = r.json().get("value", [])
        print(f"count={len(rows)}")
        for t in rows:
            print(
                f"  {t.get('Number')} Date={str(t.get('Date') or '')[:16]} "
                f"срок={str(t.get('СрокИсполнения') or '')[:10]} "
                f"исп={str(t.get('ДатаИсполнения') or '')[:10]} Ex={t.get('Executed')} "
                f"Исполнитель={t.get('Исполнитель')}"
            )
            print(f"    {(t.get('ПредметСтрокой') or '')[:140]}")
            print(f"    desc={(t.get('Description') or '')[:100]}")
            print(f"    last={(t.get('CRM_ПоследнееДействиеСтрокой') or '')[:160]}")

        # also by deadline in May
        filt2 = (
            f"РольИсполнителя_Key eq guid'{role}' and DeletionMark eq false "
            f"and СрокИсполнения ge datetime'2026-05-01T00:00:00' "
            f"and СрокИсполнения lt datetime'2026-06-01T00:00:00'"
        )
        url = (
            f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
            f"?$format=json&$filter={quote(filt2, safe='')}"
            f"&$top=100"
        )
        r = session.get(url, timeout=120)
        rows2 = r.json().get("value", []) if r.ok else []
        print(f"  by deadline May: {len(rows2)}")

    # Groups for role Начальник СУП
    print("\n=== Groups for role Начальник СУП ===")
    filt = f"РольИсполнителя_Key eq guid'{ROLE}'"
    url = (
        f"{BASE}/{quote('Catalog_ГруппыИсполнителейЗадач')}"
        f"?$format=json&$filter={quote(filt, safe='')}&$top=50"
    )
    r = session.get(url, timeout=60)
    print("HTTP", r.status_code)
    if r.ok:
        for g in r.json().get("value", []):
            print(f"  {g.get('Description')} {g.get('Ref_Key')}")
            # tasks by group in May
            gid = g["Ref_Key"]
            filt = (
                f"ГруппаИсполнителейЗадач_Key eq guid'{gid}' and DeletionMark eq false "
                f"and Date ge datetime'2026-05-01T00:00:00' "
                f"and Date lt datetime'2026-06-01T00:00:00'"
            )
            url = (
                f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
                f"?$format=json&$filter={quote(filt, safe='')}&$top=100"
            )
            rr = session.get(url, timeout=120)
            rows = rr.json().get("value", []) if rr.ok else []
            print(f"    tasks Date May: {len(rows)}")
            for t in rows[:20]:
                print(
                    f"      {t.get('Number')} Date={str(t.get('Date') or '')[:16]} "
                    f"срок={str(t.get('СрокИсполнения') or '')[:10]} "
                    f"Ex={t.get('Executed')} {(t.get('ПредметСтрокой') or '')[:100]}"
                )


if __name__ == "__main__":
    main()
