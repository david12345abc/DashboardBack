from __future__ import annotations

import json
from typing import Any

from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from .services import (
    LmStudioConnectionError,
    LmStudioModelLoadError,
    LmStudioTimeoutError,
    create_chat_completion,
    list_models,
)


def _json_response(payload: Any, *, status: int = 200) -> JsonResponse:
    return JsonResponse(
        payload,
        safe=isinstance(payload, dict),
        status=status,
        json_dumps_params={"ensure_ascii": False},
    )


def _upstream_error(exc: Exception) -> JsonResponse:
    status = 504 if isinstance(exc, LmStudioTimeoutError) else 502
    return _json_response({"error": str(exc)}, status=status)


@require_POST
def chat_completions(request):
    try:
        payload = json.loads(request.body or b"{}")
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _json_response({"error": "Invalid JSON"}, status=400)
    if not isinstance(payload, dict):
        return _json_response({"error": "JSON body must be an object"}, status=400)

    try:
        response_payload, status = create_chat_completion(payload)
    except ValueError as exc:
        return _json_response({"error": str(exc)}, status=400)
    except LmStudioModelLoadError as exc:
        return _json_response({"error": str(exc)}, status=503)
    except (LmStudioConnectionError, LmStudioTimeoutError) as exc:
        return _upstream_error(exc)
    return _json_response(response_payload, status=status)


@require_GET
def models(request):
    try:
        response_payload, status = list_models()
    except (LmStudioConnectionError, LmStudioTimeoutError) as exc:
        return _upstream_error(exc)
    return _json_response(response_payload, status=status)


@require_GET
def health(request):
    try:
        response_payload, upstream_status = list_models()
    except (LmStudioConnectionError, LmStudioTimeoutError) as exc:
        return _upstream_error(exc)

    ok = 200 <= upstream_status < 300
    return _json_response(
        {
            "ok": ok,
            "model": settings.LM_STUDIO_MODEL,
            "lm_studio_status": upstream_status,
            "models": response_payload,
        },
        status=200 if ok else 503,
    )
