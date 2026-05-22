"""Область расчёта факта текучести по HR (штат / увольнения)."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TurnoverHrScope:
    group_aliases: dict[str, list[str]]
    group_order: list[str]
    restrict_org_keys: frozenset[str] | None = None
    staff_count_structure_keys: frozenset[str] | None = None
    extra_allowed_org_keys: frozenset[str] = frozenset()
    structure_org_overrides: dict[str, str] = field(default_factory=dict)
    structure_org_skip: dict[str, str] = field(default_factory=dict)
    structure_org_aliases: dict[str, tuple[str, ...]] = field(default_factory=dict)
