from __future__ import annotations

import json

from django.http import JsonResponse, StreamingHttpResponse
from django.views.decorators.http import require_GET, require_POST

from User.views import login_required

from .services import (
    create_assistant_job,
    get_assistant_job,
    stop_assistant_job,
    stream_job_events,
)


def _current_user_context(request) -> tuple[str, dict]:
    current_user = getattr(request, 'current_user', None)
    user_label = getattr(current_user, 'nickname', '') or getattr(current_user, 'department', '') or ''
    user_context = {
        'id': getattr(current_user, 'id', None),
        'nickname': getattr(current_user, 'nickname', ''),
        'role': getattr(current_user, 'role', ''),
        'department': getattr(current_user, 'department', ''),
        'is_admin': bool(getattr(current_user, 'is_admin', False)),
    }
    return user_label, user_context


@require_POST
@login_required
def chat(request):
    try:
        payload = json.loads(request.body or b'{}')
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    message = str(payload.get('message') or '').strip()
    if not message:
        return JsonResponse({'error': 'message is required'}, status=400)

    selected_file_raw = payload.get('selected_file')
    selected_file = str(selected_file_raw).strip() if selected_file_raw else None
    room_id_raw = payload.get('room_id')
    conversation_id = str(room_id_raw).strip() if room_id_raw else None
    user_label, user_context = _current_user_context(request)
    job = create_assistant_job(
        message,
        selected_file=selected_file,
        user_label=user_label,
        user_context=user_context,
        conversation_id=conversation_id,
    )

    response = StreamingHttpResponse(
        stream_job_events(job['id'], after=-1, user_context=user_context),
        content_type='application/x-ndjson; charset=utf-8',
    )
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    response['X-AI-Job-ID'] = job['id']
    return response


@require_GET
@login_required
def job_detail(request, job_id: str):
    _user_label, user_context = _current_user_context(request)
    job = get_assistant_job(job_id, user_context)
    if job is None:
        return JsonResponse({'error': 'AI job not found'}, status=404)
    return JsonResponse({'job': job}, json_dumps_params={'ensure_ascii': False})


@require_GET
@login_required
def job_stream(request, job_id: str):
    _user_label, user_context = _current_user_context(request)
    try:
        after = int(request.GET.get('after', '-1'))
    except (TypeError, ValueError):
        after = -1
    response = StreamingHttpResponse(
        stream_job_events(job_id, after=after, user_context=user_context),
        content_type='application/x-ndjson; charset=utf-8',
    )
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response


@require_POST
@login_required
def job_stop(request, job_id: str):
    _user_label, user_context = _current_user_context(request)
    job = stop_assistant_job(job_id, user_context)
    if job is None:
        return JsonResponse({'error': 'AI job not found'}, status=404)
    return JsonResponse({'ok': True, 'job': job}, json_dumps_params={'ensure_ascii': False})
