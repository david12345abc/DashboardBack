"""Проверка алгоритма пароля 1С и сопоставления подразделений. Без базы."""

from __future__ import annotations

import unittest

from User.department_map import dashboard_leaders, match_dashboard_department
from User.erp_auth import merge_login_users
from User.onec_password import verify_password, _sha1_b64


def _blob(password: str) -> bytes:
    h1 = _sha1_b64(password)
    h2 = _sha1_b64(password.upper())
    structure = f'{{"pass","{h1}","{h2}"}}'.encode("utf-8")
    key = b"k"
    payload = bytes(byte ^ key[0] for byte in structure)
    return bytes([len(key)]) + key + payload


_CATALOG = {
    "одп": "ОДП",
    "отдел продаж бми": "Отдел продаж БМИ",
    "коммерческий директор": "коммерческий директор",
    "заместитель тех. директора по качеству": "Заместитель тех. директора по качеству",
    "председатель совета директоров": "Председатель совета директоров",
    "операционный директор": "Операционный директор",
    "главный метролог": "Главный метролог",
    "отдел вэд": "Отдел ВЭД",
    "директор по развитию": "Директор по развитию",
}


class PasswordTests(unittest.TestCase):
    def test_matching_password(self):
        self.assertTrue(verify_password(_blob("Секрет1"), "Секрет1"))

    def test_upper_variant(self):
        self.assertTrue(verify_password(_blob("Секрет1"), "секрет1".upper()))

    def test_wrong_password(self):
        self.assertFalse(verify_password(_blob("Секрет1"), "другой"))

    def test_empty(self):
        self.assertFalse(verify_password(b"", "Секрет1"))
        self.assertFalse(verify_password(_blob("Секрет1"), ""))


class DepartmentMapTests(unittest.TestCase):
    def test_alias_dealer_sales(self):
        self.assertEqual(
            match_dashboard_department("Отдел дилерских продаж", catalog=_CATALOG),
            "ОДП",
        )

    def test_exact_department(self):
        self.assertEqual(
            match_dashboard_department("Отдел продаж БМИ", catalog=_CATALOG),
            "Отдел продаж БМИ",
        )

    def test_position_when_department_unknown(self):
        self.assertEqual(
            match_dashboard_department(
                "Сектор сопровождения продаж",
                "Коммерческий директор",
                catalog=_CATALOG,
            ),
            "коммерческий директор",
        )

    def test_ancestor_does_not_grant_parent_dashboard(self):
        self.assertEqual(
            match_dashboard_department(
                "ОТК-1",
                "Контролер",
                ["ЗАМЕСТИТЕЛЬ ДИРЕКТОРА ПО КАЧЕСТВУ", "ТЕХНИЧЕСКИЙ ДИРЕКТОР"],
                catalog=_CATALOG,
            ),
            "",
        )

    def test_does_not_climb_to_chairman(self):
        self.assertEqual(
            match_dashboard_department(
                "Складской комплекс",
                "Кладовщик",
                ["Председатель Совета Директоров"],
                catalog=_CATALOG,
            ),
            "",
        )

    def test_metrology_alias(self):
        self.assertEqual(
            match_dashboard_department("Метрологическая служба", catalog=_CATALOG),
            "Главный метролог",
        )


class LeaderTests(unittest.TestCase):
    def test_ved_only_department_head(self):
        heads = dashboard_leaders([
            ("Титова Яна Владимировна", "Отдел внешнеэкономической деятельности", "Начальник отдела"),
            ("Кисличкина Наталья Викторовна", "Отдел внешнеэкономической деятельности", "Заместитель начальника отдела"),
        ])
        self.assertEqual([item["nickname"] for item in heads], ["Титова Яна Владимировна"])
        self.assertEqual(heads[0]["department"], "Отдел ВЭД")

    def test_commercial_director(self):
        heads = dashboard_leaders([
            ("Торотадзе Дмитрий Шотаевич", "КОММЕРЧЕСКИЙ ДИРЕКТОР", "Коммерческий директор"),
            ("Титова Яна Владимировна", "Отдел внешнеэкономической деятельности", "Начальник отдела"),
        ])
        by_name = {item["nickname"]: item["department"] for item in heads}
        self.assertEqual(by_name["Торотадзе Дмитрий Шотаевич"], "коммерческий директор")
        self.assertEqual(by_name["Титова Яна Владимировна"], "Отдел ВЭД")

    def test_mangasaryan_is_not_development_director(self):
        heads = dashboard_leaders([
            ("Соломичева Светлана Викторовна", "Служба развития", "Директор по развитию"),
            ("Кондратюк Михаела Борисовна", "Служба развития", "Начальник службы развития"),
            ("Мангасарян Давид Каренович", "Сектор по внедрению искусственного интеллекта", "Руководитель сектора"),
        ])
        by_name = {item["nickname"]: item["department"] for item in heads}
        self.assertEqual(by_name, {"Соломичева Светлана Викторовна": "Директор по развитию"})

    def test_other_director_is_not_chairman(self):
        self.assertEqual(
            dashboard_leaders([
                ("Амураль Игорь Борисович", "Председатель Совета Директоров", "Директор по научной работе"),
            ]),
            [],
        )

    def test_deputy_is_not_chief_metrolog(self):
        self.assertEqual(
            dashboard_leaders([
                ("Павлов Александр Сергеевич", "Метрологическая служба", "Заместитель главного метролога"),
            ]),
            [],
        )

    def test_generic_director_of_named_unit(self):
        heads = dashboard_leaders([
            ("Донцова Анна Егоровна", "ОПЕРАЦИОННЫЙ ДИРЕКТОР", "Директор"),
        ])
        self.assertEqual(len(heads), 1)
        self.assertEqual(heads[0]["department"], "Операционный директор")

    def test_deputy_keeps_dashboard_named_as_that_post(self):
        heads = dashboard_leaders([
            ("Целищев Павел Сергеевич", "Зам. директора по производству", "Заместитель директора по производству"),
        ])
        self.assertEqual(len(heads), 1)
        self.assertEqual(heads[0]["department"], "Заместитель директора по производству")

    def test_head_of_group_by_position(self):
        heads = dashboard_leaders([
            ("Тищенко Марина Николаевна", "Инспекционная группа", "Руководитель инспекционной группы"),
        ])
        self.assertEqual(len(heads), 1)
        self.assertEqual(heads[0]["department"], "Руководитель инспекционной группы")


class MergeLoginUsersTests(unittest.TestCase):
    def test_keeps_legacy_account_and_leader(self):
        users = merge_login_users(
            [{"nickname": "Титова Яна Владимировна", "department": "Отдел ВЭД", "position": "Начальник отдела"}],
            [{"nickname": "old_nick", "department": "ОДП", "position": ""}],
        )
        by_name = {item["nickname"]: item for item in users}
        self.assertEqual(set(by_name), {"Титова Яна Владимировна", "old_nick"})
        self.assertEqual(by_name["old_nick"]["department"], "ОДП")

    def test_same_login_keeps_leader_post(self):
        users = merge_login_users(
            [{"nickname": "Титова Яна Владимировна", "department": "Отдел ВЭД", "position": "Начальник отдела"}],
            [{"nickname": "титова яна владимировна", "department": "Бухгалтерия", "position": ""}],
        )
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]["department"], "Отдел ВЭД")
        self.assertEqual(users[0]["position"], "Начальник отдела")


if __name__ == "__main__":
    unittest.main()
