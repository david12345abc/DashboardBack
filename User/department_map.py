"""Сопоставление подразделения и должности из erp_pm с узлами structure.json."""

from __future__ import annotations

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

_STRUCTURE_PATH = Path(__file__).resolve().parent.parent / "getkpi" / "structure.json"

# Имя в 1С (уже нормализованное) → имя узла дашборда.
# Нужно там, где в erp_pm подразделение называется иначе, чем ключ в structure.json.
_ALIASES: dict[str, str] = {
    "отдел дилерских продаж": "ОДП",
    "одп": "ОДП",
    "отдел продаж эталонного оборудования и услуг": "Отдел ОПЭОиУ",
    "опэоиу": "Отдел ОПЭОиУ",
    "отдел внешнеэкономической деятельности": "Отдел ВЭД",
    "отдел вэд": "Отдел ВЭД",
    "отдел по работе с пао газпром": "Отдел по работе с ПАО «Газпром»",
    "отдел по работе с пао «газпром»": "Отдел по работе с ПАО «Газпром»",
    "метрологическая служба": "Главный метролог",
    "отдел метрологии и сертификации": "Главный метролог",
    "сервисная служба": "Начальник сервисной службы",
    "отдел сопровождения 1с": "Начальник отдела сопровождения 1С",
    "начальник отдела сопровождения 1с": "Начальник отдела сопровождения 1С",
    "служба развития": "Директор по развитию",
    "конструкторское бюро": "Главный конструктор",
    "отдел информационных технологий": "Начальник отдела автоматизации ИТ",
    "отдел автоматизации ит": "Начальник отдела автоматизации ИТ",
    "начальник отдела it": "Начальник отдела автоматизации ИТ",
    "ревизионная комиссия": "Руководитель ревизионной комиссии",
    "директор по качеству": "Заместитель тех. директора по качеству",
    "заместитель директора по качеству": "Заместитель тех. директора по качеству",
    "зам. технического директора по качеству": "Заместитель тех. директора по качеству",
    "заместитель технического директора по качеству": "Заместитель тех. директора по качеству",
    "директор по логистике": "Начальник службы логистики",
    "директор по производству": "Заместитель операционного директора-директор по производству",
    "директор по персоналу": "Служба управления персоналом",
    "начальник сервисной службы": "Начальник сервисной службы",
    "гспп": "Руководитель ГСПП",
}

# Слова подразделения, а не личной должности. Дашборд такого узла получает
# только самый старший руководитель внутри этого подразделения.
_ORG_UNIT_WORDS = (
    "отдел",
    "сектор",
    "служба",
    "группа",
    "бюро",
    "управление",
    "комиссия",
    "цех",
)
_ORG_UNIT_SHORT = {"одп", "опэоиу", "гспп"}

def normalize_org_name(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "")
    text = text.replace("ё", "е").replace("Ё", "Е")
    text = text.replace("«", "").replace("»", "").replace('"', "")
    text = re.sub(r"^\(ликв\.\)\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip().lower()
    text = re.sub(r"\s*-\s*", "-", text)
    return text


def _flatten(node, out: list[str]) -> None:
    if not isinstance(node, dict):
        return
    for name, children in node.items():
        if isinstance(name, str) and name.strip():
            out.append(name.strip())
        _flatten(children, out)


@lru_cache(maxsize=1)
def structure_names() -> dict[str, str]:
    """Нормализованное имя → каноническое имя из structure.json."""
    try:
        data = json.loads(_STRUCTURE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    names: list[str] = []
    _flatten(data, names)
    out: dict[str, str] = {}
    for name in names:
        out.setdefault(normalize_org_name(name), name)
    return out


def _resolve(raw: str, catalog: dict[str, str]) -> str:
    norm = normalize_org_name(raw)
    if not norm:
        return ""
    direct = catalog.get(norm)
    if direct:
        return direct
    alias = _ALIASES.get(norm)
    if not alias:
        return ""
    return catalog.get(normalize_org_name(alias), "")


def match_dashboard_department(
    department: str,
    position: str = "",
    ancestors: list[str] | None = None,
    *,
    catalog: dict[str, str] | None = None,
) -> str:
    """Вернуть узел своего подразделения или своей должности.

    Родители в оргструктуре не учитываются: сотрудник вложенного сектора
    не получает дашборд вышестоящего руководителя.
    """
    del ancestors
    names = catalog if catalog is not None else structure_names()
    for raw in (department, position):
        hit = _resolve(raw, names)
        if hit:
            return hit
    return ""


def _is_org_unit(name: str) -> bool:
    norm = normalize_org_name(name)
    if not norm or norm in _ORG_UNIT_SHORT:
        return bool(norm)
    parts = set(re.split(r"[\s,./()+-]+", norm))
    return bool(parts & set(_ORG_UNIT_WORDS))


def is_deputy_position(position: str) -> bool:
    norm = normalize_org_name(position)
    return (
        norm.startswith("заместитель")
        or norm.startswith("зам.")
        or norm.startswith("зам ")
        or norm.startswith("и.о.")
        or norm.startswith("и.о ")
        or norm.startswith("исполняющий обязанности")
    )


def position_rank(position: str) -> int:
    """Чем выше число, тем старше должность. Неруководящие — 0."""
    norm = normalize_org_name(position)
    if not norm or "помощник" in norm:
        return 0
    if "председатель" in norm:
        tier = 100
    elif re.search(r"(^| )директор(?:а|у|ом|е|ы|ов)?($| )", norm):
        tier = 90
    elif re.search(r"(^| )главн(?:ый|ая|ое|ого|ой|ым|ом)($| )", norm):
        tier = 80
    elif "начальник" in norm:
        tier = 70
    elif re.search(r"(^| )руководитель(?:я|ю|ем|е)?($| )", norm):
        tier = 60
    else:
        return 0
    if is_deputy_position(norm):
        tier -= 15
    if "1 категории" in norm:
        tier += 3
    elif "2 категории" in norm:
        tier += 2
    elif "3 категории" in norm:
        tier += 1
    return tier


def _post_matches(position: str, node: str, catalog: dict[str, str]) -> bool:
    if not position or not node:
        return False
    if normalize_org_name(position) == normalize_org_name(node):
        return True
    resolved = _resolve(position, catalog)
    return bool(resolved) and resolved == node


def _generic_director_of(position: str, node: str) -> bool:
    """В 1С должность часто записана просто «Директор», а полное имя — у подразделения."""
    if normalize_org_name(position) != "директор":
        return False
    node_norm = normalize_org_name(node)
    return node_norm == "директор" or node_norm.endswith(" директор") or node_norm.startswith("директор ")


def leadership_dashboard(
    department: str,
    position: str,
    *,
    catalog: dict[str, str] | None = None,
) -> str:
    """Какой дашборд соответствует подразделению и должности, без обхода родителей.

    Отдел, служба, сектор и т.п. — дашборд этого подразделения.
    Подразделение, названное должностью («КОММЕРЧЕСКИЙ ДИРЕКТОР»), — только если
    человек сам занимает эту должность.
    """
    names = catalog if catalog is not None else structure_names()
    dept_hit = _resolve(department, names) if _is_org_unit(department) else ""
    if dept_hit:
        return dept_hit
    pos_hit = _resolve(position, names)
    if pos_hit and _post_matches(position, pos_hit, names):
        return pos_hit
    if not _is_org_unit(department):
        titled = _resolve(department, names)
        if titled and (
            _post_matches(position, titled, names) or _generic_director_of(position, titled)
        ):
            return titled
    return ""


def dashboard_leaders(
    members: list[tuple[str, str, str]],
    *,
    catalog: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    """Оставить в каждом подразделении только самую руководящую должность.

    members: (ФИО, подразделение 1С, должность).
    Заместитель попадает в список, только если дашборд назван именно его должностью.
    """
    names = catalog if catalog is not None else structure_names()
    groups: dict[str, list[tuple[str, str, str]]] = {}
    for fio, department, position in members:
        fio = " ".join((fio or "").split())
        department = (department or "").strip()
        position = (position or "").strip()
        if not fio:
            continue
        key = normalize_org_name(department) or f"\0{normalize_org_name(position)}\0{fio.casefold()}"
        groups.setdefault(key, []).append((fio, department, position))

    chosen: dict[str, tuple[int, dict[str, str]]] = {}
    for group in groups.values():
        ranked: list[tuple[int, str, str, str]] = []
        for fio, department, position in group:
            node = leadership_dashboard(department, position, catalog=names)
            rank = position_rank(position)
            if not node or rank <= 0:
                continue
            if is_deputy_position(position) and not _post_matches(position, node, names):
                continue
            ranked.append((rank, fio, position, node))
        if not ranked:
            continue
        top = max(item[0] for item in ranked)
        for rank, fio, position, node in ranked:
            if rank != top:
                continue
            prev = chosen.get(fio.casefold())
            if prev is None or rank > prev[0]:
                chosen[fio.casefold()] = (
                    rank,
                    {"nickname": fio, "position": position, "department": node},
                )
    return [item[1] for item in sorted(chosen.values(), key=lambda item: item[1]["nickname"].casefold())]
