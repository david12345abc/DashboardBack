"""Правила отбора документа ЭкземплярБюджета и статей ФОТ / бюджетного плана."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from getkpi.budget_instance_plan import (
    AMOUNT_FIELD,
    ABSENT_ARTICLE_NOTE,
    EXCLUDED_CFO,
    EXCLUDED_FOT,
    FOT_ARTICLE_NAMES,
    NO_DOCUMENT_REASON,
    aggregate_document,
    calculate,
    choose_document,
    classify_article,
    document_covers,
    indicator_from_result,
    month_bounds,
)


def _doc(
    number: str,
    moment: str,
    *,
    scenario: str = "Плановые данные - ЦФО",
    organization: str = "НПО",
    model: str = "1 этап",
    ref: str | None = None,
) -> dict:
    return {
        "ref": ref or number,
        "number": number,
        "date": datetime.fromisoformat(moment),
        "organization_key": organization,
        "organization_name": organization,
        "scenario_key": scenario,
        "scenario_name": scenario,
        "model_key": model,
        "model_name": model,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 12, 31),
    }


class ClassifyTests(unittest.TestCase):
    def test_five_fot_names_exact(self):
        for name in FOT_ARTICLE_NAMES:
            self.assertEqual(classify_article(name), "fot")
            self.assertEqual(classify_article(f"  {name.upper()}  "), "fot")

    def test_partial_name_is_not_fot(self):
        self.assertEqual(
            classify_article("Затраты на оплату труда сотрудников (постоянная)"),
            "budget",
        )
        self.assertEqual(classify_article("АРХИВ Налог на заработную плату"), "budget")

    def test_cfo_prefix(self):
        self.assertEqual(classify_article("ЦФО"), "cfo")
        self.assertEqual(classify_article("  цфо ПЦ1"), "cfo")
        self.assertEqual(classify_article("ЦФОбезпробела"), "cfo")
        self.assertEqual(classify_article("Расходы ЦФО"), "budget")


class ChooseDocumentTests(unittest.TestCase):
    def test_latest_date_then_number(self):
        older = _doc("000001190", "2026-02-01T10:00:00")
        same_day_low = _doc("000001191", "2026-03-01T23:59:59")
        same_day_high = _doc("000001200", "2026-03-01T23:59:59")
        chosen, error = choose_document([older, same_day_low, same_day_high])
        self.assertIsNone(error)
        assert chosen is not None
        self.assertEqual(chosen["number"], "000001200")

    def test_later_time_wins_over_number(self):
        early = _doc("000009999", "2026-03-01T08:00:00")
        late = _doc("000000001", "2026-03-01T09:00:00")
        chosen, error = choose_document([early, late])
        self.assertIsNone(error)
        assert chosen is not None
        self.assertEqual(chosen["number"], "000000001")

    def test_period_must_cover(self):
        self.assertTrue(
            document_covers(date(2026, 1, 1), date(2026, 12, 31), date(2026, 1, 1), date(2026, 1, 31))
        )
        self.assertFalse(
            document_covers(date(2026, 2, 1), date(2026, 12, 31), date(2026, 1, 1), date(2026, 1, 31))
        )
        self.assertFalse(
            document_covers(date(2026, 1, 1), date(2026, 1, 31), date(2026, 1, 1), date(2026, 2, 28))
        )

    def test_empty_candidates(self):
        chosen, error = choose_document([])
        self.assertIsNone(chosen)
        self.assertEqual(error, NO_DOCUMENT_REASON)

    def test_ambiguity_lists_values_and_does_not_pick(self):
        docs = [
            _doc("1", "2026-05-01T00:00:00", scenario="Плановые данные - ЦФО"),
            _doc("2", "2026-06-01T00:00:00", scenario="Лимит по канцтоварам"),
        ]
        chosen, error = choose_document(docs)
        self.assertIsNone(chosen)
        assert error is not None
        self.assertIn("Плановые данные - ЦФО", error)
        self.assertIn("Лимит по канцтоварам", error)
        self.assertIn("сценарий", error)

    def test_scenario_filter_then_latest(self):
        docs = [
            _doc("1", "2026-05-01T00:00:00", scenario="Плановые данные - ЦФО"),
            _doc("2", "2026-06-01T00:00:00", scenario="Лимит по канцтоварам"),
            _doc("3", "2026-04-01T00:00:00", scenario="Плановые данные - ЦФО"),
        ]
        chosen, error = choose_document(docs, scenario="плановые данные - цфо")
        self.assertIsNone(error)
        assert chosen is not None
        self.assertEqual(chosen["number"], "1")


class AggregateTests(unittest.TestCase):
    def setUp(self):
        self.start = date(2026, 1, 1)
        self.end = date(2026, 1, 31)

    def test_fot_budget_cfo_and_missing_article(self):
        analytics = [
            {"line_id": "a", "article_name": "Затраты на оплату труда (постоянная)"},
            {"line_id": "b", "article_name": "Затраты на оплату труда (постоянная)"},
            {"line_id": "c", "article_name": "Подбор персонала"},
            {"line_id": "d", "article_name": "ЦФО ПЦ1"},
            {"line_id": "e", "article_name": "Налог на заработную плату"},
            {"line_id": "f", "article_name": "Полиграфия"},
        ]
        turnovers = [
            {"line_id": "a", "period": date(2026, 1, 1), "amount": 100},
            {"line_id": "b", "period": date(2026, 1, 1), "amount": -20},
            {"line_id": "a", "period": date(2026, 2, 1), "amount": 999},
            {"line_id": "c", "period": date(2026, 1, 1), "amount": 50},
            {"line_id": "d", "period": date(2026, 1, 1), "amount": 70},
            {"line_id": "e", "period": date(2026, 1, 1), "amount": 10},
        ]
        result = aggregate_document(analytics, turnovers, self.start, self.end)
        by_name = {row["name"]: row for row in result["fot_articles"]}
        self.assertEqual(len(by_name), 5)
        self.assertEqual(by_name["Затраты на оплату труда (постоянная)"]["amount"], 80.0)
        self.assertFalse(by_name["Затраты на оплату труда (постоянная)"]["missing"])
        self.assertTrue(by_name["Затраты на оплату труда (переменная)"]["missing"])
        self.assertEqual(
            by_name["Затраты на оплату труда (переменная)"]["note"],
            ABSENT_ARTICLE_NOTE,
        )
        self.assertEqual(result["fot_total"], 90.0)
        self.assertEqual(
            [(row["name"], row["amount"]) for row in result["budget_plan_articles"]],
            [("Подбор персонала", 50.0), ("Полиграфия", 0.0)],
        )
        self.assertEqual(result["budget_plan_total"], 50.0)
        reasons = {(row["name"], row["reason"]): row["amount"] for row in result["excluded_articles"]}
        self.assertEqual(reasons[("ЦФО ПЦ1", EXCLUDED_CFO)], 70.0)
        self.assertEqual(reasons[("Затраты на оплату труда (постоянная)", EXCLUDED_FOT)], 80.0)
        self.assertEqual(reasons[("Налог на заработную плату", EXCLUDED_FOT)], 10.0)
        self.assertIn("f", {row["line_id"] for row in result["lines_without_turnover"]})
        self.assertEqual(result["amount_field"] if "amount_field" in result else AMOUNT_FIELD, AMOUNT_FIELD)

    def test_fot_articles_are_excluded_from_budget_even_when_named_in_analytics(self):
        analytics = [
            {"line_id": "a", "article_name": "Налог на заработную плату"},
            {"line_id": "b", "article_name": "  ЦФО "},
        ]
        turnovers = [
            {"line_id": "a", "period": date(2026, 1, 1), "amount": 5},
            {"line_id": "b", "period": date(2026, 1, 1), "amount": 8},
        ]
        result = aggregate_document(analytics, turnovers, self.start, self.end)
        self.assertEqual(result["budget_plan_articles"], [])
        self.assertEqual(result["budget_plan_total"], 0.0)
        self.assertEqual(result["fot_total"], 5.0)
        self.assertEqual(result["excluded_articles"][0]["reason"], EXCLUDED_CFO)

    def test_orphan_turnover_is_not_added(self):
        result = aggregate_document(
            [],
            [{"line_id": "missing", "period": date(2026, 1, 1), "amount": 1000}],
            self.start,
            self.end,
        )
        self.assertEqual(result["fot_total"], 0.0)
        self.assertEqual(result["budget_plan_total"], 0.0)
        self.assertEqual(result["turnovers_without_article"][0]["amount"], 1000.0)

    def test_currency_field_is_not_a_second_addend(self):
        result = aggregate_document(
            [{"line_id": "a", "article_name": "Подбор персонала"}],
            [{"line_id": "a", "period": date(2026, 1, 1), "amount": 15, "СуммаВВалюте": 999}],
            self.start,
            self.end,
        )
        self.assertEqual(result["budget_plan_total"], 15.0)


class IndicatorTests(unittest.TestCase):
    def test_missing_document_is_left_out_of_the_sum(self):
        result = {
            "total_complete": False,
            "fot_total": None,
            "budget_plan_total": None,
            "departments": [
                {"department": "А", "document": {"number": "1"}, "fot_total": 10.0, "budget_plan_total": 3.0, "reason": None},
                {"department": "Б", "document": None, "fot_total": None, "budget_plan_total": None, "reason": NO_DOCUMENT_REASON},
            ],
        }
        self.assertEqual(indicator_from_result(result, "fot"), 10.0)

    def test_unknown_department_blocks_the_total(self):
        result = {
            "total_complete": False,
            "fot_total": None,
            "departments": [
                {"department": "А", "document": {"number": "1"}, "fot_total": 10.0, "reason": None},
                {"department": "Б", "document": None, "fot_total": None, "reason": "подразделение не найдено"},
            ],
        }
        self.assertIsNone(indicator_from_result(result, "fot"))

    def test_ambiguity_blocks_the_total(self):
        result = {
            "total_complete": False,
            "fot_total": None,
            "departments": [
                {
                    "department": "А",
                    "document": None,
                    "fot_total": None,
                    "reason": "нельзя выбрать документ, в данных несколько значений: сценарий: А; Б",
                }
            ],
        }
        self.assertIsNone(indicator_from_result(result, "fot"))


class CalculateContractTests(unittest.TestCase):
    def test_empty_department_list_is_not_zero(self):
        result = calculate([], date(2026, 1, 1), date(2026, 1, 31))
        self.assertIsNone(result["fot_total"])
        self.assertIsNone(result["budget_plan_total"])
        self.assertFalse(result["total_complete"])
        self.assertEqual(result["departments"], [])
        self.assertEqual(result["amount_field"], AMOUNT_FIELD)

    def test_month_bounds_are_inclusive_calendar_month(self):
        self.assertEqual(month_bounds(2026, 1), (date(2026, 1, 1), date(2026, 1, 31)))
        self.assertEqual(month_bounds(2024, 2), (date(2024, 2, 1), date(2024, 2, 29)))


if __name__ == "__main__":
    unittest.main()
