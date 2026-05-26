from __future__ import annotations

import json

from django.http import JsonResponse, StreamingHttpResponse
from django.views.decorators.http import require_POST

from User.views import login_required

from .services import stream_agent_answer


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
    current_user = getattr(request, 'current_user', None)
    user_label = getattr(current_user, 'nickname', '') or getattr(current_user, 'department', '') or ''

    response = StreamingHttpResponse(
        stream_agent_answer(message, selected_file=selected_file, user_label=user_label),
        content_type='application/x-ndjson; charset=utf-8',
    )
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response
