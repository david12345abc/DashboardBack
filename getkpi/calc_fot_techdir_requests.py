"""
calc_fot_techdir_requests.py — факт ФОТ техдиректора по заявкам на расходование ДС.

Алгоритм:
  1. Берём документы «Заявка на расходование ДС».
  2. Включаем только документы со статусом полной или частичной оплаты.
  3. Исключаем заявки без оплаты / аннулированные / не проведённые.
  4. Берём строки заявки, где подразделение однозначно сопоставляется с перечнем
     техдиректора.
  5. Если по строкам есть собственная оплата, используем её.
     Если оплаты на строках нет, распределяем оплаченный документ пропорционально
     суммам строк.
  6. ФОТ техдиректора = сумма по 19 подразделениям.

Важно:
  - Скрипт не использует список статей затрат, т.к. в этом ТЗ он не задан.
  - Правило распределения при отсутствии детализации по строкам:
    "doc_paid_amount_proportional_by_line_amount".
"""

from __future__ import annotations

import functools
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from getkpi import calc_budget_techdir_m3 as req  # noqa: E402
from getkpi import fot_techdir_fact as fts  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

MONTH_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def parse_period() -> tuple[int, int, str, str]:
    period_arg = None
    for arg in sys.argv[1:]:
        if len(arg) == 7 and arg[4] == "-":
            period_arg = arg
            break

    if period_arg:
        year, month = int(period_arg[:4]), int(period_arg[5:7])
    else:
        year, month = _last_full_month()

    if month == 12:
        p_start = f"{year}-12-01T00:00:00"
        p_end = f"{year + 1}-01-01T00:00:00"
    else:
        p_start = f"{year}-{month:02d}-01T00:00:00"
        p_end = f"{year}-{month + 1:02d}-01T00:00:00"
    return year, month, p_start, p_end


def compute_td_fot_request_fact(year: int, month: int) -> dict:
    p_start, p_end = req._month_period_bounds(year, month)

    session = requests.Session()
    session.auth = req.AUTH

    print("  [1/5] Загрузка структуры предприятия...")
    structure_rows, by_key, _, exact_index = fts.load_structure(session)
    print(f"        загружено строк структуры: {len(structure_rows)}")
    print("  [2/5] Поиск эталонных подразделений...")
    name_to_key, name_to_structure_label = fts.load_fot_spec_structure_map(session)
    print(f"        сопоставлено подразделений: {len(name_to_key)}/{len(fts.FOT_GROUP_ORDER)}")
    struct_map = fts.build_struct_key_to_fot_group(name_to_key, by_key)

    print("  [3/5] Определение сущностей документа и строк...")
    doc_entity, tab_entity = req._discover_request_entities(session)
    print(f"        документ: {doc_entity}")
    print(f"        строки:   {tab_entity or '<из документа>'}")
    print("  [4/5] Загрузка заявок за период...")
    docs, doc_debug = req._fetch_request_docs(session, doc_entity, p_start, p_end)
    print(f"        документов найдено: {len(docs)}")

    totals_by_group: dict[str, float] = defaultdict(float)
    totals_by_group_docs: dict[str, int] = defaultdict(int)
    totals_by_group_lines: dict[str, int] = defaultdict(int)
    included_docs: list[dict] = []

    counts = {
        "docs_total": len(docs),
        "docs_included": 0,
        "docs_skipped_status": 0,
        "docs_skipped_no_paid_amount": 0,
        "docs_skipped_empty": 0,
        "lines_total": 0,
        "lines_included": 0,
        "lines_skipped_dept": 0,
        "lines_skipped_empty": 0,
        "allocation_rule": "doc_paid_amount_proportional_by_line_amount",
    }

    total_fact = 0.0

    print("  [5/5] Обработка документов и распределение сумм...")
    for idx, doc in enumerate(docs, start=1):
        if idx % 25 == 0:
            print(f"        обработано документов: {idx}/{len(docs)}")
        if doc.get("DeletionMark") or doc.get("Deleted"):
            counts["docs_skipped_status"] += 1
            continue
        if doc.get("Posted") is False:
            counts["docs_skipped_status"] += 1
            continue

        doc_ref = req._normalize(doc.get("Ref_Key")).lower()
        if not doc_ref or doc_ref == fts.EMPTY:
            counts["docs_skipped_empty"] += 1
            continue

        lines = req._extract_request_lines_from_doc(doc)
        if not lines and tab_entity:
            lines = req._fetch_request_lines(session, tab_entity, doc_ref)
        if not lines:
            counts["docs_skipped_empty"] += 1
            continue

        status = req._normalize(req._pick_first(doc, req.REQUEST_HEADER_STATUS_FIELDS))
        status_kind = req._classify_request_status(status)
        has_line_paid_values = any((req._line_paid_amount(line) or 0) > 0 for line in lines)
        if status_kind == "unpaid" and not has_line_paid_values:
            counts["docs_skipped_status"] += 1
            continue

        base_total = sum(req._line_amount(line) for line in lines)
        if base_total <= 0:
            counts["docs_skipped_empty"] += 1
            continue

        doc_paid_amount, paid_source = req._extract_doc_paid_amount(doc)
        if doc_paid_amount is None and status_kind == "full":
            doc_paid_amount = base_total
            paid_source = "status_full_request_amount"
        elif has_line_paid_values:
            paid_source = "line_paid_amounts"
        elif doc_paid_amount is None and not has_line_paid_values:
            counts["docs_skipped_no_paid_amount"] += 1
            continue

        if doc_paid_amount is not None:
            doc_paid_amount = max(0.0, min(float(doc_paid_amount), float(base_total)))

        counts["docs_included"] += 1
        counts["lines_total"] += len(lines)
        included_docs.append(
            {
                "ref": doc_ref,
                "number": req._normalize(doc.get("Number")),
                "date": req._normalize(doc.get("Date"))[:10],
                "status": status,
                "status_kind": status_kind,
                "paid_amount": round(doc_paid_amount or 0.0, 2),
                "paid_source": paid_source,
                "lines": len(lines),
            }
        )

        doc_allocated = 0.0
        for line in lines:
            group_name = None
            for raw_dept in req._request_department_candidates(session, doc, line):
                group_name = req._resolve_request_group(raw_dept, struct_map, structure_rows, exact_index)
                if group_name:
                    break
            if not group_name:
                counts["lines_skipped_dept"] += 1
                continue

            line_amount = req._line_amount(line)
            if line_amount <= 0:
                counts["lines_skipped_empty"] += 1
                continue

            line_paid_amount = req._line_paid_amount(line)
            if line_paid_amount is not None and line_paid_amount > 0:
                allocated = float(line_paid_amount)
            elif doc_paid_amount is not None:
                allocated = round(doc_paid_amount * (line_amount / base_total), 2)
            else:
                continue

            totals_by_group[group_name] += allocated
            totals_by_group_docs[group_name] += 1
            totals_by_group_lines[group_name] += 1
            total_fact += allocated
            doc_allocated += allocated
            counts["lines_included"] += 1

        if doc_paid_amount is not None and doc_allocated > 0:
            diff = round(float(doc_paid_amount) - float(doc_allocated), 2)
            if abs(diff) >= 0.01:
                counts.setdefault("doc_allocation_diff_total", 0.0)
                counts["doc_allocation_diff_total"] = round(
                    float(counts.get("doc_allocation_diff_total", 0.0)) + diff,
                    2,
                )

    groups_out: dict[str, dict] = {}
    for group_name in fts.FOT_GROUP_ORDER:
        groups_out[group_name] = {
            "structure_label": name_to_structure_label.get(group_name),
            "fact_total": round(totals_by_group.get(group_name, 0.0), 2),
            "rows": totals_by_group_lines.get(group_name, 0),
            "docs": totals_by_group_docs.get(group_name, 0),
        }

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month],
        "period": {
            "start": p_start[:10],
            "end": p_end[:10],
        },
        "groups": groups_out,
        "total_fact": round(total_fact, 2),
        "counts": counts,
        "debug": {
            "doc_entity": doc_entity,
            "tab_entity": tab_entity,
            "doc_debug": doc_debug,
            "allocation_rule": "doc_paid_amount_proportional_by_line_amount",
            "department_map_size": len(struct_map),
            "structure_rows": len(structure_rows),
            "by_key_rows": len(by_key),
            "target_departments": list(fts.FOT_GROUP_ORDER),
        },
        "included_docs": included_docs[:50],
    }


def main() -> None:
    year, month, p_start, p_end = parse_period()
    t0 = time.time()

    print(f"\n{'=' * 100}")
    print(f"  ФАКТ ФОТ ТЕХДИРЕКЦИИ ПО ЗАЯВКАМ ДС — {MONTH_RU[month]} {year}")
    print(f"  Период: {p_start[:10]} — {p_end[:10]} (lt)")
    print(f"{'=' * 100}")

    snapshot = compute_td_fot_request_fact(year, month)

    print(f"  Документов загружено: {snapshot['counts']['docs_total']}")
    print(f"  Документов включено: {snapshot['counts']['docs_included']}")
    print(
        f"  Пропущено: по статусу={snapshot['counts']['docs_skipped_status']}, "
        f"без суммы оплаты={snapshot['counts']['docs_skipped_no_paid_amount']}, "
        f"пустые={snapshot['counts']['docs_skipped_empty']}"
    )
    print(
        f"  Строк включено: {snapshot['counts']['lines_included']}, "
        f"пропущено без подразделения={snapshot['counts']['lines_skipped_dept']}"
    )

    print(f"\n{'=' * 100}")
    print(f"  ФОТ ТЕХДИРЕКЦИИ ПО ЗАЯВКАМ ДС — {MONTH_RU[month]} {year}")
    print(f"{'=' * 100}")
    print(f"  {'Подразделение':<52} {'ФОТ':>16} {'Заявок':>10} {'Строк':>10}")
    print(f"  {'-' * 52} {'-' * 16} {'-' * 10} {'-' * 10}")

    for group_name in fts.FOT_GROUP_ORDER:
        group = snapshot["groups"][group_name]
        print(
            f"  {group_name:<52} "
            f"{group['fact_total']:>16,.2f} "
            f"{group['docs']:>10} "
            f"{group['rows']:>10}"
        )

    print(f"  {'-' * 52} {'-' * 16} {'-' * 10} {'-' * 10}")
    print(f"  {'ИТОГО по техдирекции':<52} {snapshot['total_fact']:>16,.2f}")

    if snapshot["counts"].get("doc_allocation_diff_total") is not None:
        print(
            f"\n  Расхождение округления по документам: "
            f"{snapshot['counts']['doc_allocation_diff_total']:,.2f}"
        )

    print(f"\n  Алгоритм распределения: {snapshot['debug']['allocation_rule']}")
    print(f"  Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
