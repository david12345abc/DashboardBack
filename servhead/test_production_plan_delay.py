"""SH-M8: отставание только если срок по плану уже прошёл, а задача не закрыта."""

from __future__ import annotations

import unittest
from datetime import date, datetime
from types import SimpleNamespace

from servhead.production_plan import _delay_as_of, _is_behind_plan


def _task(percent, finish):
    return SimpleNamespace(percent_complete=percent, finish=finish)


class DelayRuleTests(unittest.TestCase):
    def test_closed_month_is_judged_on_its_last_day(self):
        self.assertEqual(_delay_as_of(2026, 9, date(2026, 10, 13)), date(2026, 9, 30))

    def test_current_month_excludes_tasks_due_today(self):
        self.assertEqual(_delay_as_of(2026, 10, date(2026, 10, 13)), date(2026, 10, 12))

    def test_open_task_with_passed_finish_is_a_delay(self):
        task = _task(40, datetime(2026, 9, 15, 18, 0))
        self.assertTrue(_is_behind_plan(task, date(2026, 9, 30)))

    def test_finished_task_is_not_a_delay(self):
        task = _task(100, datetime(2026, 9, 15, 18, 0))
        self.assertFalse(_is_behind_plan(task, date(2026, 9, 30)))

    def test_open_task_with_future_finish_is_not_a_delay(self):
        task = _task(0, datetime(2026, 10, 20, 18, 0))
        self.assertFalse(_is_behind_plan(task, date(2026, 9, 30)))

    def test_task_due_on_the_as_of_day_is_a_delay(self):
        task = _task(0, date(2026, 9, 30))
        self.assertTrue(_is_behind_plan(task, date(2026, 9, 30)))

    def test_missing_finish_is_not_a_delay(self):
        self.assertFalse(_is_behind_plan(_task(0, None), date(2026, 9, 30)))
        self.assertFalse(_is_behind_plan(_task(0, datetime(1899, 12, 30)), date(2026, 9, 30)))


if __name__ == "__main__":
    unittest.main()
