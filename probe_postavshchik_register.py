"""
probe_postavshchik_register.py — диагностика регистра
РасчетыСПоставщикамиПоДокументам.

Аналог probe_klient_register.py: печатает все поля сущности (через
$metadata), 2 примера записей и статистику по RecordType × суммам
всех числовых ресурсов на указанную дату.

Запуск на сервере:
  python probe_postavshchik_register.py                 # на 2026-03-31
  python probe_postavshchik_register.py 2026-03-31
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
REGISTER = "AccumulationRegister_РасчетыСПоставщикамиПоДокументам_RecordType"
ENTITY_META = "AccumulationRegister_РасчетыСПоставщикамиПоДокументам"


def print_header(title: str):
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def fetch_metadata(session: requests.Session) -> str | None:
    url = f"{BASE}/$metadata"
    r = session.get(url, timeout=120)
    if not r.ok:
        print(f"  ⚠ $metadata HTTP {r.status_code}")
        return None
    return r.text


def extract_entity_schema(xml: str, entity_name: str) -> list[str]:
    pattern = rf'<EntityType[^>]*Name="{re.escape(entity_name)}"[^>]*>(.*?)</EntityType>'
    m = re.search(pattern, xml, re.DOTALL)
    if not m:
        return []
    body = m.group(1)
    props = re.findall(r'<Property[^>]*Name="([^"]+)"[^>]*Type="([^"]+)"', body)
    return [f"{name}  :  {typ}" for name, typ in props]


def fetch_sample(session: requests.Session, n: int = 2) -> list[dict]:
    url = f"{BASE}/{REGISTER}?$format=json&$top={n}"
    r = session.get(url, timeout=120)
    if not r.ok:
        print(f"  ⚠ sample HTTP {r.status_code}")
        return []
    return r.json().get("value", [])


def fetch_stats(session: requests.Session, date_str: str) -> tuple[dict, dict]:
    counts_rt: dict[str, int] = defaultdict(int)
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
            f"{BASE}/{REGISTER}"
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
            rt = row.get("RecordType", "") or ""
            counts_rt[rt] += 1
            for k, v in row.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    numeric_keys.add(k)
                    sums[rt][k] += float(v)

        total += len(batch)
        if total % 50000 == 0:
            print(f"  обработано {total}…")
        if len(batch) < PAGE:
            break
        skip += PAGE

    print(f"  всего записей: {total}")
    print(f"  найдено числовых полей: {sorted(numeric_keys)}")
    return counts_rt, sums


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-03-31"

    session = requests.Session()
    session.auth = AUTH

    print_header(f"$metadata: список полей {ENTITY_META}")
    xml = fetch_metadata(session)
    if xml:
        for ent_name in (f"{ENTITY_META}", f"{ENTITY_META}_RecordType"):
            props = extract_entity_schema(xml, ent_name)
            if props:
                print(f"\n  Entity: {ent_name}")
                for p in props:
                    print(f"    {p}")

    print_header("Пример 2 записей регистра (все поля)")
    sample = fetch_sample(session, n=2)
    for i, row in enumerate(sample, 1):
        print(f"\n  запись #{i}:")
        print(json.dumps(row, ensure_ascii=False, indent=2))

    print_header(f"Статистика по регистру на {date_str} (RecordType × сумма ресурсов)")
    counts, sums = fetch_stats(session, date_str)
    print("\n  Количество записей по RecordType:")
    for rt, cnt in sorted(counts.items()):
        print(f"    {rt!r}: {cnt}")
    print("\n  Сумма числовых полей по RecordType:")
    for rt in sorted(sums.keys()):
        print(f"\n    RecordType = {rt!r}")
        for key in sorted(sums[rt].keys()):
            val = sums[rt][key]
            print(f"      {key:<40s} = {val:>20,.2f}")


if __name__ == "__main__":
    main()
