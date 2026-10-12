"""Проверка пароля 1С по полю v8users.Data.

Алгоритм как в Constructor (tools/onec/password.py):
первый байт — длина ключа, далее ключ, остальное XOR с ключом,
в расшифрованной структуре два SHA-1 (пароль и пароль в верхнем регистре).
"""

from __future__ import annotations

import base64
import hashlib
import re


def decode_password_structure(data: bytes) -> str:
    if not data:
        raise ValueError("Empty Data blob")
    key_size = data[0]
    if key_size <= 0 or key_size + 1 > len(data):
        raise ValueError(f"Invalid key size: {key_size}")
    key = data[1 : key_size + 1]
    payload = data[key_size + 1 :]
    decoded = bytearray(len(payload))
    j = 0
    for i, byte in enumerate(payload):
        decoded[i] = byte ^ key[j]
        j += 1
        if j >= key_size:
            j = 0
    return decoded.decode("utf-8", errors="replace")


def _sha1_b64(password: str) -> str:
    digest = hashlib.sha1(password.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("ascii")


def _extract_pass_hashes(structure: str) -> tuple[str, str]:
    quoted = re.findall(r'"([^"]*)"', structure)
    hashes = [
        q
        for q in quoted
        if re.fullmatch(r"[A-Za-z0-9+/]{27}=", q) or re.fullmatch(r"[A-Za-z0-9+/]{28}", q)
    ]
    if len(hashes) >= 2:
        return hashes[0], hashes[1]
    if len(hashes) == 1:
        return hashes[0], ""
    raw = re.findall(r"[A-Za-z0-9+/]{27}=", structure)
    if len(raw) >= 2:
        return raw[0], raw[1]
    if len(raw) == 1:
        return raw[0], ""
    return "", ""


def verify_password(data: bytes, password: str) -> bool:
    """True, если пароль совпадает с хешами в блобе Data пользователя 1С."""
    if not data or not password:
        return False
    try:
        structure = decode_password_structure(data)
    except ValueError:
        return False
    hash1, hash2 = _extract_pass_hashes(structure)
    if not hash1 and not hash2:
        return False
    expected = _sha1_b64(password)
    expected_upper = _sha1_b64(password.upper())
    stored = {h for h in (hash1, hash2) if h}
    return expected in stored or expected_upper in stored
