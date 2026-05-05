"""
Фасад для плиток ПЦ1/ПЦ2 заместителя операционного директора.

Функциональные блоки вынесены:
  - calc_prod_deputy_pc_budget.py — бюджет ПЦ1/ПЦ2;
  - calc_prod_deputy_pc_fot.py — ФОТ ПЦ1/ПЦ2;
  - calc_prod_deputy_pc_common.py — планы, кэш и агрегация.
"""
from __future__ import annotations

from .calc_prod_deputy_pc_budget import get_pc_budget_monthly
from .calc_prod_deputy_pc_common import ShopKey
from .calc_prod_deputy_pc_fot import get_pc_fot_monthly

__all__ = ["ShopKey", "get_pc_budget_monthly", "get_pc_fot_monthly"]
