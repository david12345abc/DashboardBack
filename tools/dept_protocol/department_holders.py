"""Поиск ФИО на должностях по source и пути matched_1c в кадровых данных 1С."""

from __future__ import annotations

from list_enterprise_positions import normalize_text

LEADERSHIP_HINTS = (
    "директор",
    "председатель",
    "начальник",
    "главный",
    "заместитель",
    "помощник",
)
STOP_WORDS = frozenset({"по", "и", "в", "на", "с", "для", "от", "the"})
MAX_SUBTREE_FOR_ALL = 12


def normalize_dept_path(path: str | None) -> str:
    text = normalize_text(path)
    if text.startswith("оргструктура "):
        return text[len("оргструктура ") :].strip()
    return text


def paths_for_scope(matched_path: str) -> list[str]:
    parts = [part.strip() for part in matched_path.split("/") if part.strip()]
    paths: list[str] = []
    while parts:
        paths.append(" / ".join(parts))
        parts.pop()
    return paths


def rows_for_matched_path(all_rows: list[dict], matched_path: str) -> tuple[list[dict], str]:
    for path in paths_for_scope(matched_path):
        scoped = [row for row in all_rows if department_in_scope(row["department"], path)]
        if scoped:
            return scoped, path
    return [], matched_path


def dept_tokens(path: str | None) -> list[str]:
    return normalize_dept_path(path).split()


def department_in_scope(dept_path: str, root_path: str) -> bool:
    dept = dept_tokens(dept_path)
    root = dept_tokens(root_path)
    if not root:
        return False
    if len(dept) < len(root):
        return False
    return dept[: len(root)] == root


def holder_record(row: dict) -> dict:
    return {
        "fio": row["employee"],
        "position": row["position"],
        "department": row["department"],
        "since": (row.get("period") or "")[:10],
    }


def path_last_segment(path: str) -> str:
    return normalize_text(path.rsplit("/", 1)[-1])


def meaningful_tokens(text: str) -> set[str]:
    return {token for token in normalize_text(text).split() if token not in STOP_WORDS}


def expand_source_variants(source: str) -> list[str]:
    norm = normalize_text(source)
    variants = [norm]
    if norm == "одп":
        variants.append("отдел дилерских продаж")
    expanded = norm.replace("тех.", "технического").replace("зам.", "заместитель")
    if expanded != norm:
        variants.append(expanded)
    return variants


def is_leadership_source(source_norm: str) -> bool:
    return any(hint in source_norm for hint in LEADERSHIP_HINTS)


def token_overlap(left: str, right: str) -> float:
    left_tokens = meaningful_tokens(left)
    right_tokens = meaningful_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / min(len(left_tokens), len(right_tokens))


def position_score(source_variants: list[str], position: str) -> float:
    pos_norm = normalize_text(position)
    best = 0.0
    for source in source_variants:
        if source == pos_norm:
            return 1.0
        if source in pos_norm or pos_norm in source:
            best = max(best, 0.9)
            continue
        score = token_overlap(source, pos_norm)
        if is_leadership_source(source) and any(h in pos_norm for h in LEADERSHIP_HINTS):
            score += 0.1
        if "директор" in source and "директор" not in pos_norm:
            score -= 0.35
        if "начальник" in source and "начальник" not in pos_norm:
            score -= 0.25
        best = max(best, score)
    return best


def mandatory_position_markers(source_norm: str) -> list[frozenset[str]]:
    markers: list[frozenset[str]] = []
    if "логистик" in source_norm:
        markers.append(frozenset({"логистик"}))
    if "коммерч" in source_norm:
        markers.append(frozenset({"коммерч"}))
    if "операционн" in source_norm and "директор" in source_norm:
        markers.append(frozenset({"операционн"}))
    if "качеств" in source_norm:
        markers.append(frozenset({"качеств"}))
    if (
        "заместитель" in source_norm
        and "операционн" in source_norm
        and "производств" in source_norm
    ):
        markers.append(frozenset({"производств", "операционн"}))
    return markers


def satisfies_position_markers(position: str, markers: list[frozenset[str]]) -> bool:
    if not markers:
        return True
    pos_norm = normalize_text(position)
    return all(any(marker in pos_norm for marker in group) for group in markers)


def rank_by_position(
    source: str,
    rows: list[dict],
    *,
    min_score: float,
    markers: list[frozenset[str]] | None = None,
) -> list[dict]:
    if markers is None:
        markers = mandatory_position_markers(normalize_text(source))
    variants = expand_source_variants(source)
    scored = [(position_score(variants, row["position"]), row) for row in rows]
    scored.sort(key=lambda item: (-item[0], item[1]["employee"]))
    return [
        row
        for score, row in scored
        if score >= min_score and satisfies_position_markers(row["position"], markers)
    ]


def select_holders(source: str, onec_path: str, rows: list[dict]) -> tuple[list[dict], str]:
    source_norm = normalize_text(source)
    path_tokens = dept_tokens(onec_path)
    last_seg = path_last_segment(onec_path)
    direct = [r for r in rows if dept_tokens(r["department"]) == path_tokens]
    leadership_source = is_leadership_source(source_norm)

    def pack(items: list[dict], method: str) -> tuple[list[dict], str]:
        unique: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for row in items:
            key = (row["employee"], row["position"])
            if key in seen:
                continue
            seen.add(key)
            unique.append(holder_record(row))
        return unique, method

    by_position = [r for r in rows if normalize_text(r["position"]) == source_norm]
    if by_position:
        return pack(by_position, "position_exact")

    if last_seg and last_seg == source_norm:
        on_node = [r for r in direct if normalize_text(r["position"]) == last_seg]
        if on_node:
            return pack(on_node, "node_title")

    fuzzy = [
        r
        for r in rows
        if source_norm in normalize_text(r["position"])
        or normalize_text(r["position"]) in source_norm
    ]
    if fuzzy:
        markers = mandatory_position_markers(source_norm)
        if markers:
            fuzzy = [r for r in fuzzy if satisfies_position_markers(r["position"], markers)]
        if fuzzy:
            if leadership_source:
                fuzzy = [
                    r
                    for r in fuzzy
                    if any(h in normalize_text(r["position"]) for h in LEADERSHIP_HINTS)
                ] or fuzzy
            return pack(fuzzy, "position_fuzzy")

    markers = mandatory_position_markers(source_norm)
    if "начальник" in source_norm:
        if "логистик" in last_seg:
            heads = [
                r
                for r in direct
                if normalize_text(r["position"])
                in {"начальник службы", "начальник службы логистики"}
            ]
            if heads:
                return pack(heads, "department_head")
        exact_chiefs = [
            r
            for r in direct
            if normalize_text(r["position"])
            in {"начальник отдела", "начальник службы", "начальник управления"}
        ]
        if exact_chiefs:
            return pack(exact_chiefs[:3], "department_head")
        chief_rows = rank_by_position(source, direct or rows, min_score=0.45, markers=markers)
        chief_rows = [
            r for r in chief_rows if "начальник" in normalize_text(r["position"])
        ]
        if chief_rows:
            return pack(chief_rows[:3], "department_head")

    if source_norm.startswith("отдел") or source_norm == "одп":
        heads = [
            r
            for r in direct
            if normalize_text(r["position"])
            in {"начальник отдела", "начальник управления"}
        ]
        if heads:
            return pack(heads, "department_head")

    if leadership_source:
        best = rank_by_position(source, direct or rows, min_score=0.55, markers=markers)
        best = [
            r
            for r in best
            if any(h in normalize_text(r["position"]) for h in LEADERSHIP_HINTS)
        ]
        if best:
            return pack(best[:3], "leadership_best_match")

    if direct:
        leaders = [
            r
            for r in direct
            if any(h in normalize_text(r["position"]) for h in LEADERSHIP_HINTS)
        ]
        if leaders:
            return pack(leaders, "node_leadership")
        if not leadership_source and len(direct) <= MAX_SUBTREE_FOR_ALL:
            return pack(direct, "department_all")

    return [], "not_found"
