"""Правила плиток «всего» и «в работе» без SQL."""

from __future__ import annotations

import unittest
from datetime import date

from qualdir.form_sla import (
    FormHit,
    add_working_days,
    aggregate_hits,
    as_in_work_payload,
    deadline_date,
    in_work_on_time,
    weekday_is_working,
)


class FormSlaTests(unittest.TestCase):
    def test_two_working_days_skip_weekend(self):
        friday = date(2026, 10, 9)
        self.assertEqual(friday.weekday(), 4)
        self.assertEqual(add_working_days(friday, 2, weekday_is_working), date(2026, 10, 13))
        saturday = date(2026, 10, 10)
        self.assertEqual(add_working_days(saturday, 2, weekday_is_working), date(2026, 10, 13))

    def test_deadline_is_32_calendar_days_from_creation(self):
        created = date(2026, 10, 9)
        self.assertEqual(deadline_date(created, weekday_is_working), date(2026, 11, 10))

    def test_overdue_boundary_and_execution_status(self):
        created = date(2026, 10, 9)
        last_on_plan = date(2026, 11, 9)
        first_overdue = date(2026, 11, 10)
        self.assertEqual((last_on_plan - created).days, 31)
        self.assertEqual((first_overdue - created).days, 32)
        self.assertIs(
            True,
            in_work_on_time("НаСогласовании", created, last_on_plan, weekday_is_working),
        )
        self.assertIs(
            False,
            in_work_on_time("СогласованиеКМ", created, first_overdue, weekday_is_working),
        )
        self.assertIs(
            True,
            in_work_on_time("ИсполнениеКМ", created, date(2027, 1, 1), weekday_is_working),
        )
        self.assertIsNone(in_work_on_time("Выполнено", created, last_on_plan, weekday_is_working))
        self.assertIsNone(in_work_on_time("Подготовлен", created, last_on_plan, weekday_is_working))
        self.assertIsNone(in_work_on_time("Отменена", created, last_on_plan, weekday_is_working))

    def test_month_is_creation_month(self):
        hits = [
            FormHit(
                created=date(2026, 1, 30),
                status="НаСогласовании",
                elimination=None,
                dept_name="ОТК",
                significant=False,
                doc_id=b"1",
                plan_month="2026-01",
                fact_month=None,
            ),
            FormHit(
                created=date(2026, 2, 3),
                status="Выполнено",
                elimination=date(2026, 3, 11),
                dept_name="ОТК",
                significant=True,
                doc_id=b"2",
                plan_month="2026-02",
                fact_month="2026-02",
            ),
            FormHit(
                created=date(2026, 1, 20),
                status="Выполнено",
                elimination=date(2026, 2, 11),
                dept_name="ОТК",
                significant=False,
                doc_id=b"4",
                plan_month="2026-01",
                fact_month="2026-01",
            ),
            FormHit(
                created=date(2025, 12, 30),
                status="НаСогласовании",
                elimination=None,
                dept_name="ОТК",
                significant=False,
                doc_id=b"5",
                plan_month="2025-12",
                fact_month=None,
            ),
            FormHit(
                created=date(2025, 12, 30),
                status="ИсполнениеКМ",
                elimination=None,
                dept_name="Цех",
                significant=False,
                doc_id=b"6",
                plan_month="2025-12",
                fact_month=None,
            ),
            FormHit(
                created=date(2026, 1, 5),
                status="РазработкаКМ",
                elimination=None,
                dept_name="Цех",
                significant=False,
                doc_id=b"3",
                plan_month="2026-01",
                fact_month=None,
            ),
        ]
        stats = aggregate_hits(
            hits,
            ["2026-01", "2026-02"],
            dept_key=lambda name: name or "—",
            today=date(2026, 2, 28),
            is_working=weekday_is_working,
        )
        self.assertEqual(stats["2026-02"]["plan"], 1)
        self.assertEqual(stats["2026-02"]["fact"], 1)
        self.assertEqual(stats["2026-01"]["plan"], 3)
        self.assertEqual(stats["2026-01"]["fact"], 1)
        # Устранение в феврале не переносит январскую «Завершено» из января.
        # Формы от 30.12 остаются в декабре и в январь не входят.
        self.assertEqual(stats["2026-01"]["in_work_on_time"], 2)
        self.assertEqual(stats["2026-01"]["in_work_overdue"], 0)
        self.assertEqual(stats["2026-02"]["in_work_on_time"], 0)
        self.assertEqual(stats["2026-02"]["in_work_overdue"], 0)

    def test_in_work_payload_uses_on_time_as_fact(self):
        payload = as_in_work_payload(
            {
                "data_granularity": "monthly",
                "kpi_period": {"type": "last_full_month", "year": 2026, "month": 2, "month_name": "Февраль"},
                "monthly_data": [
                    {
                        "month": 2,
                        "year": 2026,
                        "month_name": "февраль",
                        "in_work_on_time": 8,
                        "in_work_overdue": 2,
                        "in_work_departments": [{"name": "ОТК", "count": 10}],
                    }
                ],
            },
            "QD-M8W",
        )
        row = payload["monthly_data"][0]
        self.assertEqual(row["plan"], 10)
        self.assertEqual(row["fact"], 8)
        self.assertEqual(row["overdue"], 2)
        self.assertEqual(row["kpi_pct"], 80.0)
        self.assertEqual(payload["ytd"]["total_plan"], 10)


if __name__ == "__main__":
    unittest.main()
