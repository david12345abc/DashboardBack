"""
probe_supplier_candidates.py — детальная диагностика двух регистров-кандидатов
для задолженности поставщиков:
  1) РасчетыСПоставщикамиПланОплат      — живой, Period до 2026
  2) РасчетыСПоставщиками                — похож на регистр расчётов

Для каждого печатает:
  * полный список полей из $metadata;
  * 3 самые свежие записи по убыванию Period;
  * общее количество записей Active=true с Period <= 2026-03-31;
  * сумму всех числовых полей по RecordType на 2026-03-31.

Запуск:
  python probe_supplier_candidates.py
  python probe_supplier_candidates.py 2026-03-31
"""

import json
import re
import sys
from collections import defaultdict
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

CANDIDATES = [
    ("РасчетыСПоставщикамиПланОплат",
     "AccumulationRegister_РасчетыСПоставщикамиПланОплат",
     "AccumulationRegister_РасчетыСПоставщикамиПланОплат_RecordType"),
    ("РасчетыСПоставщиками",
     "AccumulationRegister_РасчетыСПоставщиками",
     "AccumulationRegister_РасчетыСПоставщиками_RecordType"),
]


def print_header(title: str):
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def fetch_metadata(session: requests.Session) -> str:
    r = session.get(f"{BASE}/$metadata", timeout=180)
    r.raise_for_status()
    return r.text


def extract_schema(xml: str, entity: str) -> list[tuple[str, str]]:
    m = re.search(
        rf'<EntityType[^>]*Name="{re.escape(entity)}"[^>]*>(.*?)</EntityType>',
        xml, re.DOTALL,
    )
    if not m:
        return []
    body = m.group(1)
    return re.findall(r'<Property[^>]*Name="([^"]+)"[^>]*Type="([^"]+)"', body)


def fetch_last(session: requests.Session, entity_rt: str, n: int = 3) -> list[dict]:
    url = (
        f"{BASE}/{entity_rt}"
        f"?$format=json&$top={n}"
        f"&$orderby=Period%20desc"
        f"&$filter=Active%20eq%20true"
    )
    r = session.get(url, timeout=120)
    if not r.ok:
        print(f"  ⚠ fetch_last HTTP {r.status_code}")
        return []
    return r.json().get("value", [])


def fetch_stats(session: requests.Session, entity_rt: str, date_str: str):
    counts: dict[str, int] = defaultdict(int)
    sums: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    flt = quote(
        f"Period le datetime'{date_str}T23:59:59' and Active eq true",
        safe="",
    )
    PAGE = 5000
    skip = 0
    total = 0
    numeric_keys: set[str] = set()

    while True:
        url = (
            f"{BASE}/{entity_rt}"
            f"?$format=json"
            f"&$filter={flt}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = session.get(url, timeout=180)
        if not r.ok:
            print(f"  ⚠ stats HTTP {r.status_code} skip={skip}")
            break
        batch = r.json().get("value", [])
        if not batch:
            break
        for row in batch:
            rt = row.get("RecordType", "") or "<no RT>"
            counts[rt] += 1
            for k, v in row.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    numeric_keys.add(k)
                    sums[rt][k] += float(v)
        total += len(batch)
        if total % 50000 == 0:
            print(f"    обработано {total}…")
        if len(batch) < PAGE:
            break
        skip += PAGE

    return total, counts, sums, numeric_keys


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-03-31"

    session = requests.Session()
    session.auth = AUTH

    print_header("Загружаем $metadata (один раз)")
    xml = fetch_metadata(session)
    print("  ok, длина =", len(xml))

    for short, entity_flat, entity_rt in CANDIDATES:
        print_header(f"РЕГИСТР: {short}")

        print(f"\n--- $metadata ({entity_rt}) ---")
        props = extract_schema(xml, entity_rt)
        if not props:
            props = extract_schema(xml, entity_flat)
        for name, typ in props:
            print(f"  {name:<40s}: {typ}")

        print(f"\n--- 3 последние записи (Period desc, Active=true) ---")
        last_rows = fetch_last(session, entity_rt, n=3)
        for i, row in enumerate(last_rows, 1):
            compact = {k: v for k, v in row.items() if not k.endswith("@navigationLinkUrl")}
            print(f"\n  запись #{i}:")
            print(json.dumps(compact, ensure_ascii=False, indent=2))

        print(f"\n--- Статистика на {date_str} (Active=true, Period<=cutoff) ---")
        total, counts, sums, numeric_keys = fetch_stats(session, entity_rt, date_str)
        print(f"  всего записей: {total}")
        print(f"  числовые поля: {sorted(numeric_keys)}")
        print("\n  Кол-во записей по RecordType:")
        for rt, cnt in sorted(counts.items()):
            print(f"    {rt!r}: {cnt}")
        print("\n  Сумма числовых полей по RecordType:")
        for rt in sorted(sums.keys()):
            print(f"\n    RecordType = {rt!r}")
            for key in sorted(sums[rt].keys()):
                v = sums[rt][key]
                print(f"      {key:<35s} = {v:>20,.2f}")


if __name__ == "__main__":
    main()
