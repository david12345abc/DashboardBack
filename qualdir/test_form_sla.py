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
    plan_bucket_date,
    weekday_is_working,
)


class FormSlaTests(unittest.TestCase):
    def test_two_working_days_skip_weekend(self):
        friday = date(2026, 10, 9)
        self.assertEqual(friday.weekday(), 4)
        self.assertEqual(add_working_days(friday, 2, weekday_is_working), date(2026, 10, 13))
        saturday = date(2026, 10, 10)
        self.assertEqual(add_working_days(saturday, 2, weekday_is_working), date(2026, 10, 13))

    def test_deadline_is_plan_date_plus_30_calendar_days(self):
        created = date(2026, 10, 9)
        self.assertEqual(plan_bucket_date(created, weekday_is_working), date(2026, 10, 13))
        self.assertEqual(deadline_date(created, weekday_is_working), date(2026, 11, 12))

    def test_overdue_boundary_and_execution_status(self):
        created = date(2026, 10, 9)
        deadline = date(2026, 11, 12)
        self.assertIs(
            True,
            in_work_on_time("НаСогласовании", created, deadline, weekday_is_working),
        )
        self.assertIs(
            False,
            in_work_on_time("СогласованиеКМ", created, date(2026, 11, 13), weekday_is_working),
        )
        self.assertIs(
            True,
            in_work_on_time("ИсполнениеКМ", created, date(2027, 1, 1), weekday_is_working),
        )
        self.assertIsNone(in_work_on_time("Выполнено", created, deadline, weekday_is_working))
        self.assertIsNone(in_work_on_time("Подготовлен", created, deadline, weekday_is_working))
        self.assertIsNone(in_work_on_time("Отменена", created, deadline, weekday_is_working))

    def test_plan_and_fact_use_different_dates(self):
        hits = [
            FormHit(
                created=date(2026, 1, 30),  # Friday → plan Monday 2026-02-02
                status="НаСогласовании",
                elimination=None,
                dept_name="ОТК",
                significant=False,
                doc_id=b"1",
                plan_month="2026-02",
                fact_month=None,
            ),
            FormHit(
                created=date(2025, 11, 3),
                status="Выполнено",
                elimination=date(2026, 2, 11),
                dept_name="ОТК",
                significant=True,
                doc_id=b"2",
                plan_month=None,
                fact_month="2026-02",
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
        self.assertEqual(stats["2026-01"]["plan"], 1)
        self.assertEqual(stats["2026-01"]["fact"], 0)
        # В месяц входят только формы с plan_month этого месяца.
        # Январская РазработкаКМ не раздувает февраль, даже если к концу февраля она просрочена.
        self.assertEqual(stats["2026-01"]["in_work_on_time"], 1)
        self.assertEqual(stats["2026-01"]["in_work_overdue"], 0)
        self.assertEqual(stats["2026-02"]["in_work_on_time"], 1)
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
