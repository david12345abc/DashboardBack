"""
probe_find_supplier_register.py — найти «правильный» регистр
задолженности поставщиков.

Делает 3 вещи:
  1) из $metadata печатает ВСЕ AccumulationRegister_* с именем,
     содержащим «Поставщ», «Закуп», «Контрагент», «Расчет»;
  2) для каждого такого регистра (flat вариант с `_RecordType`) тянет
     1 последнюю запись (по убыванию Period), чтобы увидеть, заполняется ли
     он оперативно и какой у него Recorder_Type;
  3) для подозрительных на «правильный» регистров (РасчетыСПоставщикамиПоСрокам,
     ЗадолженностьПередПоставщиками, …) — печатает ещё и максимум Period.

Запуск:
  python probe_find_supplier_register.py
"""

import json
import re

import requests
from requests.auth import HTTPBasicAuth

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

# Ключевые слова в именах регистров, которые нас интересуют.
KEYWORDS = ("Поставщ", "Закуп", "Кредитор", "Контрагент", "Расчет")


def fetch_metadata(session: requests.Session) -> str:
    r = session.get(f"{BASE}/$metadata", timeout=180)
    r.raise_for_status()
    return r.text


def find_register_entities(xml: str) -> list[str]:
    """
    Возвращает имена сущностей AccumulationRegister_...<Имя>_RecordType,
    имя которых содержит одно из KEYWORDS.
    """
    names = re.findall(r'<EntityType\s+Name="(AccumulationRegister_[^"]+)"', xml)
    result = []
    seen = set()
    for n in names:
        if not n.endswith("_RecordType"):
            continue
        short = n[len("AccumulationRegister_"):-len("_RecordType")]
        if not any(kw in short for kw in KEYWORDS):
            continue
        if short in seen:
            continue
        seen.add(short)
        result.append(n)
    return result


def fetch_last(session: requests.Session, entity: str) -> dict | None:
    """
    Вернуть запись с самым большим Period (последнюю по времени).
    """
    url = (
        f"{BASE}/{entity}"
        f"?$format=json&$top=1"
        f"&$orderby=Period%20desc"
        f"&$filter=Active%20eq%20true"
    )
    try:
        r = session.get(url, timeout=60)
        if not r.ok:
            return {"_error": f"HTTP {r.status_code}"}
        val = r.json().get("value", [])
        return val[0] if val else None
    except Exception as e:  # noqa: BLE001
        return {"_error": str(e)}


def main():
    session = requests.Session()
    session.auth = AUTH

    print("=" * 80)
    print("Поиск регистров задолженности поставщиков в $metadata")
    print("=" * 80)
    xml = fetch_metadata(session)
    entities = find_register_entities(xml)
    print(f"\nНайдено регистров, подходящих по ключевым словам: {len(entities)}\n")
    for e in entities:
        short = e[len("AccumulationRegister_"):-len("_RecordType")]
        print(f"  • {short}")

    print()
    print("=" * 80)
    print("Последняя запись каждого регистра (по Period desc, Active=true)")
    print("=" * 80)

    for entity in entities:
        short = entity[len("AccumulationRegister_"):-len("_RecordType")]
        print()
        print(f"--- {short} " + "-" * (72 - len(short)))
        row = fetch_last(session, entity)
        if row is None:
            print("  (записей нет)")
            continue
        if isinstance(row, dict) and "_error" in row:
            print(f"  ошибка: {row['_error']}")
            continue
        # Покажем компактно: Period, RecordType, Recorder_Type и числовые поля.
        compact = {
            k: v for k, v in row.items()
            if not k.endswith("@navigationLinkUrl")
        }
        print(json.dumps(compact, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
