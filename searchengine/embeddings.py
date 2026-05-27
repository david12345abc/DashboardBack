"""
Векторный индекс подразделений на базе sentence-transformers.

Модель: paraphrase-multilingual-MiniLM-L12-v2 (хорошая поддержка русского).
Модель и индекс загружаются при старте сервера. Индекс пересоздаётся
при изменении structure.json (по mtime файла).
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
STRUCTURE_FILE = Path(__file__).resolve().parent.parent / "getkpi" / "structure.json"

_PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


def _clear_unsupported_proxy_env() -> None:
    """httpx/huggingface_hub не понимают socks4:// — ломают загрузку модели при VPN."""
    for key in _PROXY_ENV_KEYS:
        value = os.environ.get(key, "")
        if not value:
            continue
        lowered = value.lower()
        if lowered.startswith(("socks4://", "socks5://")):
            logger.warning(
                "Отключён прокси %s=%s для загрузки модели embeddings (неподдерживаемая схема)",
                key,
                value,
            )
            os.environ.pop(key, None)


def _configure_hf_http_client() -> None:
    """Отключить системный/socks-прокси для Hugging Face (VPN на Windows)."""
    _clear_unsupported_proxy_env()
    try:
        import httpx
        from huggingface_hub import set_client_factory
        from huggingface_hub.utils._http import hf_request_event_hook

        def _client_factory() -> httpx.Client:
            return httpx.Client(
                event_hooks={"request": [hf_request_event_hook]},
                follow_redirects=True,
                timeout=None,
                trust_env=False,
            )

        set_client_factory(_client_factory)
    except Exception as exc:
        logger.debug("HF client factory not configured: %s", exc)


_configure_hf_http_client()

from sentence_transformers import SentenceTransformer  # noqa: E402


def _load_sentence_transformer() -> SentenceTransformer:
    local_only = os.getenv("HF_HUB_OFFLINE", "").strip().lower() in {"1", "true", "yes"}
    return SentenceTransformer(MODEL_NAME, local_files_only=local_only)


_lock = threading.Lock()
_dept_names: list[str] = []
_dept_embeddings: np.ndarray | None = None
_structure_mtime: float | None = None

logger.info("Loading embedding model %s …", MODEL_NAME)
_model = _load_sentence_transformer()
logger.info("Model loaded.")


def _collect_all_keys(tree) -> list[str]:
    """Рекурсивно собирает все названия подразделений из дерева (порядок обхода)."""
    result: list[str] = []
    if isinstance(tree, dict):
        for key, children in tree.items():
            result.append(key)
            result.extend(_collect_all_keys(children))
    elif isinstance(tree, list):
        for item in tree:
            if isinstance(item, str):
                result.append(item)
            elif isinstance(item, dict):
                result.extend(_collect_all_keys(item))
    return result


def _ensure_index():
    """Возвращает (dept_names, embeddings_matrix), пересоздаёт если structure.json изменился."""
    global _dept_names, _dept_embeddings, _structure_mtime

    current_mtime = STRUCTURE_FILE.stat().st_mtime
    if _dept_embeddings is not None and current_mtime == _structure_mtime:
        return _dept_names, _dept_embeddings

    with open(STRUCTURE_FILE, encoding="utf-8") as f:
        tree = json.load(f)

    names = list(dict.fromkeys(_collect_all_keys(tree)))

    embs = _model.encode(names, convert_to_numpy=True, normalize_embeddings=True)

    _dept_names = names
    _dept_embeddings = embs
    _structure_mtime = current_mtime
    logger.info("Built embedding index for %d departments.", len(names))
    return _dept_names, _dept_embeddings


MIN_SCORE = 0.35
SUBSTRING_BOOST = 0.25


def search(query: str, allowed: set[str], top_k: int = 5) -> list[dict]:
    """
    Гибридный поиск подразделений: cosine similarity + бонус за вхождение подстроки.

    Результаты ниже MIN_SCORE после бустинга отсекаются.
    """
    with _lock:
        names, embs = _ensure_index()

    q_emb = _model.encode([query], convert_to_numpy=True, normalize_embeddings=True)

    cos_scores: np.ndarray = (embs @ q_emb.T).flatten()

    query_lower = query.lower()
    allowed_lower = {a.lower() for a in allowed}

    candidates: list[tuple[str, float]] = []
    for idx, name in enumerate(names):
        if name.lower() not in allowed_lower:
            continue
        score = float(cos_scores[idx])
        if query_lower in name.lower():
            score = min(score + SUBSTRING_BOOST, 1.0)
        if score >= MIN_SCORE:
            candidates.append((name, score))

    candidates.sort(key=lambda x: x[1], reverse=True)

    return [
        {"department": name, "score": round(sc, 4)}
        for name, sc in candidates[:top_k]
    ]
