from __future__ import annotations

import os


API_BASE = os.environ.get("TURBOPROJECT_API_BASE", "http://192.168.1.236:8000").rstrip("/")
EMAIL = os.environ.get("TURBOPROJECT_EMAIL", "sktb_razvitie6@turbo-don.ru")
PASSWORD = os.environ.get("TURBOPROJECT_PASSWORD", "Ruslandavletov28")
TIMEOUT = int(os.environ.get("TURBOPROJECT_TIMEOUT", "60"))
