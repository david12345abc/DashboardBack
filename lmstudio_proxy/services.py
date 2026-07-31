from __future__ import annotations

import re
import threading
from typing import Any

import requests
from django.conf import settings


CHAT_OPTION_FIELDS = {
    "frequency_penalty",
    "logit_bias",
    "max_tokens",
    "presence_penalty",
    "response_format",
    "seed",
    "stop",
    "temperature",
    "tool_choice",
    "tools",
    "top_p",
}

EMBEDDING_OPTION_FIELDS = {
    "dimensions",
    "encoding_format",
    "user",
}


class LmStudioConnectionError(RuntimeError):
    pass


class LmStudioTimeoutError(RuntimeError):
    pass


class LmStudioModelLoadError(RuntimeError):
    pass


_model_load_lock = threading.Lock()


def _base_url() -> str:
    return str(settings.LM_STUDIO_BASE_URL).rstrip("/")


def _native_base_url() -> str:
    base_url = _base_url()
    if base_url.endswith("/v1"):
        base_url = base_url[:-3]
    return f"{base_url.rstrip('/')}/api/v1"


def _headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    api_key = str(settings.LM_STUDIO_API_KEY or "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _response_payload(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return {"error": response.text or f"LM Studio returned HTTP {response.status_code}"}


def _request_url(method: str, url: str, **kwargs: Any) -> tuple[Any, int]:
    try:
        response = requests.request(
            method,
            url,
            headers=_headers(),
            timeout=settings.LM_STUDIO_TIMEOUT_SECONDS,
            **kwargs,
        )
    except requests.Timeout as exc:
        raise LmStudioTimeoutError("LM Studio request timed out") from exc
    except requests.RequestException as exc:
        raise LmStudioConnectionError(f"LM Studio is unavailable: {exc}") from exc
    return _response_payload(response), response.status_code


def _request(method: str, path: str, **kwargs: Any) -> tuple[Any, int]:
    return _request_url(
        method,
        f"{_base_url()}/{path.lstrip('/')}",
        **kwargs,
    )


def _native_request(method: str, path: str, **kwargs: Any) -> tuple[Any, int]:
    return _request_url(
        method,
        f"{_native_base_url()}/{path.lstrip('/')}",
        **kwargs,
    )


def _error_message(payload: Any, fallback: str) -> str:
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            return str(error.get("message") or error.get("type") or fallback)
        if error:
            return str(error)
        if payload.get("message"):
            return str(payload["message"])
    return fallback


def _compact_model_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _model_matches(model: dict[str, Any], model_name: str) -> bool:
    candidates = {
        str(model.get("key") or "").strip(),
        str(model.get("id") or "").strip(),
        str(model.get("path") or "").strip(),
        str(model.get("display_name") or "").strip(),
    }
    target = model_name.strip()
    if not target:
        return False
    if target in candidates:
        return True

    target_l = target.lower()
    if any(
        candidate.lower().endswith(target_l) or target_l.endswith(candidate.lower())
        for candidate in candidates
        if candidate
    ):
        return True

    # HF repo id like Content-AI/USER-bge-m3-Q8_0-GGUF ↔ LM Studio key
    # text-embedding-user-bge-m3 (publisher + compact name overlap).
    publisher = str(model.get("publisher") or "").strip().lower()
    repo_name = target_l.split("/", 1)[-1]
    repo_core = re.sub(r"[-_]?gguf$", "", repo_name, flags=re.I)
    repo_core = re.sub(r"[-_]?q\d+(?:[_-][a-z0-9]+)?$", "", repo_core, flags=re.I)
    target_compact = _compact_model_token(repo_core)
    if len(target_compact) < 6:
        return False

    model_compact = _compact_model_token(
        " ".join(candidate for candidate in candidates if candidate)
    )
    if target_compact in model_compact or model_compact in target_compact:
        if "/" not in target_l or not publisher:
            return True
        publisher_compact = _compact_model_token(publisher)
        return (
            publisher_compact in _compact_model_token(target_l)
            or publisher_compact in model_compact
        )
    return False


def _list_native_models() -> list[dict[str, Any]]:
    models_payload, models_status = _native_request("GET", "models")
    if not 200 <= models_status < 300:
        raise LmStudioModelLoadError(
            _error_message(models_payload, "Could not inspect LM Studio models")
        )

    if not isinstance(models_payload, dict):
        return []
    raw_models = models_payload.get("models", models_payload.get("data", []))
    if not isinstance(raw_models, list):
        return []
    return [model for model in raw_models if isinstance(model, dict)]


def resolve_model_id(model_name: str) -> str:
    """Map configured name/HF id to the LM Studio model key used by the API."""
    target_model = model_name.strip()
    if not target_model:
        raise LmStudioModelLoadError("Model name is empty")

    target = next(
        (
            model
            for model in _list_native_models()
            if _model_matches(model, target_model)
        ),
        None,
    )
    if target is None:
        raise LmStudioModelLoadError(
            f'Model "{target_model}" is not installed in LM Studio'
        )
    return str(target.get("key") or target.get("id") or target_model)


def ensure_model_loaded(
    model_name: str | None = None,
    *,
    context_length: int | None = None,
) -> str:
    """Ensure model is loaded; returns the resolved LM Studio model key."""
    target_model = (model_name or settings.LM_STUDIO_MODEL).strip()
    target_context = (
        context_length
        if context_length is not None
        else settings.LM_STUDIO_CONTEXT_LENGTH
    )

    with _model_load_lock:
        target = next(
            (
                model
                for model in _list_native_models()
                if _model_matches(model, target_model)
            ),
            None,
        )
        if target is None:
            raise LmStudioModelLoadError(
                f'Model "{target_model}" is not installed in LM Studio'
            )

        load_key = str(target.get("key") or target.get("id") or target_model)
        if not settings.LM_STUDIO_AUTO_LOAD_MODEL:
            return load_key
        if target.get("state") == "loaded" or target.get("loaded_instances"):
            return load_key

        load_payload, load_status = _native_request(
            "POST",
            "models/load",
            json={
                "model": load_key,
                "context_length": target_context,
            },
        )
        if not 200 <= load_status < 300:
            raise LmStudioModelLoadError(
                _error_message(
                    load_payload,
                    f'Failed to load model "{target_model}"',
                )
            )
        return load_key


def create_chat_completion(client_payload: dict[str, Any]) -> tuple[Any, int]:
    messages = client_payload.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ValueError("messages must be a non-empty array")
    if client_payload.get("stream") is True:
        raise ValueError("streaming is not supported by this endpoint")

    model_id = ensure_model_loaded(settings.LM_STUDIO_MODEL)

    upstream_payload = {
        key: client_payload[key]
        for key in CHAT_OPTION_FIELDS
        if key in client_payload
    }
    upstream_payload.update(
        {
            "model": model_id,
            "messages": messages,
            "stream": False,
        }
    )
    return _request("POST", "chat/completions", json=upstream_payload)


def _normalize_embedding_input(raw_input: Any) -> list[str] | str:
    if isinstance(raw_input, str):
        if not raw_input.strip():
            raise ValueError("input must be a non-empty string or array of strings")
        return raw_input
    if isinstance(raw_input, list):
        if not raw_input:
            raise ValueError("input must be a non-empty string or array of strings")
        texts: list[str] = []
        for item in raw_input:
            if not isinstance(item, str) or not item.strip():
                raise ValueError("input array items must be non-empty strings")
            texts.append(item)
        return texts
    raise ValueError("input must be a non-empty string or array of strings")


def create_embeddings(client_payload: dict[str, Any]) -> tuple[Any, int]:
    """Proxy OpenAI-compatible embeddings to LM Studio (GGUF embedder)."""
    if "input" not in client_payload:
        raise ValueError("input is required")

    embedding_input = _normalize_embedding_input(client_payload.get("input"))
    embedding_model = str(settings.LM_STUDIO_EMBEDDING_MODEL or "").strip()
    if not embedding_model:
        raise ValueError("LM_STUDIO_EMBEDDING_MODEL is not configured")

    model_id = ensure_model_loaded(
        embedding_model,
        context_length=settings.LM_STUDIO_EMBEDDING_CONTEXT_LENGTH,
    )

    upstream_payload = {
        key: client_payload[key]
        for key in EMBEDDING_OPTION_FIELDS
        if key in client_payload
    }
    upstream_payload.update(
        {
            "model": model_id,
            "input": embedding_input,
        }
    )
    return _request("POST", "embeddings", json=upstream_payload)


def list_models() -> tuple[Any, int]:
    return _request("GET", "models")
