"""Загрузка модулей dept_protocol (локальные import без package prefix)."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_ROOT_STR = str(_ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)
