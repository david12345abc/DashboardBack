from __future__ import annotations

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


def ensure_model_loaded() -> None:
    if not settings.LM_STUDIO_AUTO_LOAD_MODEL:
        return

    with _model_load_lock:
        models_payload, models_status = _native_request("GET", "models")
        if not 200 <= models_status < 300:
            raise LmStudioModelLoadError(
                _error_message(models_payload, "Could not inspect LM Studio models")
            )

        models: list[Any] = []
        if isinstance(models_payload, dict):
            raw_models = models_payload.get("models", models_payload.get("data", []))
            if isinstance(raw_models, list):
                models = raw_models
        target = next(
            (
                model
                for model in models
                if isinstance(model, dict)
                and (model.get("key") or model.get("id")) == settings.LM_STUDIO_MODEL
            ),
            None,
        )
        if target is None:
            raise LmStudioModelLoadError(
                f'Model "{settings.LM_STUDIO_MODEL}" is not installed in LM Studio'
            )
        if target.get("state") == "loaded" or target.get("loaded_instances"):
            return

        load_payload, load_status = _native_request(
            "POST",
            "models/load",
            json={
                "model": settings.LM_STUDIO_MODEL,
                "context_length": settings.LM_STUDIO_CONTEXT_LENGTH,
            },
        )
        if not 200 <= load_status < 300:
            raise LmStudioModelLoadError(
                _error_message(
                    load_payload,
                    f'Failed to load model "{settings.LM_STUDIO_MODEL}"',
                )
            )


def create_chat_completion(client_payload: dict[str, Any]) -> tuple[Any, int]:
    messages = client_payload.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ValueError("messages must be a non-empty array")
    if client_payload.get("stream") is True:
        raise ValueError("streaming is not supported by this endpoint")

    ensure_model_loaded()

    upstream_payload = {
        key: client_payload[key]
        for key in CHAT_OPTION_FIELDS
        if key in client_payload
    }
    upstream_payload.update(
        {
            "model": settings.LM_STUDIO_MODEL,
            "messages": messages,
            "stream": False,
        }
    )
    return _request("POST", "chat/completions", json=upstream_payload)


def list_models() -> tuple[Any, int]:
    return _request("GET", "models")
