import json

from django.conf import settings
from django.core.mail import EmailMessage
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST, require_http_methods

from User.views import admin_required, login_required

from .models import FeedbackRequest


def _feedback_request_to_dict(req: FeedbackRequest) -> dict:
    return {
        'id': req.id,
        'topic': req.topic,
        'topic_label': req.get_topic_display(),
        'status': req.status,
        'status_label': req.get_status_display(),
        'user': req.user.nickname if req.user else None,
        'department': req.department,
        'related_department': req.related_department,
        'dashboard': req.dashboard,
        'period': req.period,
        'description': req.description,
        'contact': req.contact,
        'attachment_names': req.attachment_names or [],
        'email_error': req.email_error,
        'created_at': req.created_at.isoformat(),
    }


def _feedback_topic_label(topic: str) -> str:
    return dict(FeedbackRequest.Topic.choices).get(topic, topic or 'Обращение')


def _send_feedback_email(req: FeedbackRequest, files) -> None:
    subject = f'[Дашборд] {_feedback_topic_label(req.topic)}'
    body = '\n'.join([
        f'Тема: {_feedback_topic_label(req.topic)}',
        f'Пользователь: {req.user.nickname if req.user else "—"}',
        f'Подразделение пользователя: {req.department or "—"}',
        f'Относится к подразделению: {req.related_department or req.department or "—"}',
        f'Дашборд: {req.dashboard or "—"}',
        f'Период: {req.period or "—"}',
        f'Контакт: {req.contact or "—"}',
        '',
        'Описание:',
        req.description,
    ])
    message = EmailMessage(
        subject=subject,
        body=body,
        from_email=settings.DEFAULT_FROM_EMAIL,
        to=[settings.FEEDBACK_EMAIL_TO],
    )
    for uploaded in files:
        uploaded.seek(0)
        message.attach(
            uploaded.name,
            uploaded.read(),
            uploaded.content_type or 'application/octet-stream',
        )
    message.send(fail_silently=False)


def _parse_json_body(request) -> tuple[dict | None, JsonResponse | None]:
    try:
        return json.loads(request.body or b'{}'), None
    except json.JSONDecodeError:
        return None, JsonResponse({'error': 'Invalid JSON'}, status=400)


def _send_guide_support_email(department: str, full_name: str, question: str) -> None:
    message = EmailMessage(
        subject='[Дашборд] Вопрос из руководства пользователя',
        body='\n'.join([
            'Источник: страница руководства пользователя',
            f'Отдел: {department}',
            f'ФИО: {full_name}',
            '',
            'Вопрос:',
            question,
        ]),
        from_email=settings.DEFAULT_FROM_EMAIL,
        to=[settings.FEEDBACK_EMAIL_TO],
    )
    message.send(fail_silently=False)


@require_POST
def guide_support_request(request):
    data, error = _parse_json_body(request)
    if error:
        return error

    department = (data.get('department') or '').strip()
    full_name = (data.get('full_name') or data.get('fio') or '').strip()
    question = (data.get('question') or '').strip()

    if not department or not full_name or not question:
        return JsonResponse({'error': 'department, full_name and question are required'}, status=400)

    try:
        _send_guide_support_email(department, full_name, question)
    except Exception as exc:
        return JsonResponse({'error': str(exc), 'email_sent': False}, status=502)

    return JsonResponse({'ok': True, 'email_sent': True})


@require_http_methods(['GET', 'POST'])
@login_required
def feedback_requests(request):
    if request.method == 'GET':
        qs = FeedbackRequest.objects.filter(user=request.current_user)
        return JsonResponse({'requests': [_feedback_request_to_dict(req) for req in qs]})

    topic = (request.POST.get('topic') or FeedbackRequest.Topic.QUESTION).strip()
    if topic not in FeedbackRequest.Topic.values:
        topic = FeedbackRequest.Topic.QUESTION
    description = (request.POST.get('description') or '').strip()
    if not description:
        return JsonResponse({'error': 'description is required'}, status=400)

    department = (request.current_user.department or '').strip()
    related_department = (request.POST.get('related_department') or department).strip()
    files = request.FILES.getlist('attachments')
    req = FeedbackRequest.objects.create(
        user=request.current_user,
        topic=topic,
        department=department,
        related_department=related_department,
        dashboard=(request.POST.get('dashboard') or '').strip(),
        period=(request.POST.get('period') or '').strip(),
        description=description,
        contact=(request.POST.get('contact') or '').strip(),
        attachment_names=[f.name for f in files],
    )
    try:
        _send_feedback_email(req, files)
    except Exception as exc:
        req.status = FeedbackRequest.Status.FAILED
        req.email_error = str(exc)
        req.save(update_fields=['status', 'email_error'])
        return JsonResponse({'request': _feedback_request_to_dict(req), 'email_sent': False}, status=201)

    req.status = FeedbackRequest.Status.SENT
    req.save(update_fields=['status'])
    return JsonResponse({'request': _feedback_request_to_dict(req), 'email_sent': True}, status=201)


@require_GET
@admin_required
def list_feedback_requests(request):
    archived = request.GET.get('archive', '').strip().lower() in {'1', 'true', 'yes'}
    archive_statuses = [FeedbackRequest.Status.COMPLETED, FeedbackRequest.Status.REJECTED]
    qs = FeedbackRequest.objects.all()
    if archived:
        qs = qs.filter(status__in=archive_statuses)
    else:
        qs = qs.exclude(status__in=archive_statuses)
    return JsonResponse({'requests': [_feedback_request_to_dict(req) for req in qs]})


def _get_feedback_or_404(request_id):
    try:
        return FeedbackRequest.objects.get(id=request_id), None
    except FeedbackRequest.DoesNotExist:
        return None, JsonResponse({'error': 'Feedback request not found'}, status=404)


@require_POST
@admin_required
def complete_feedback_request(request, request_id):
    req, error = _get_feedback_or_404(request_id)
    if error:
        return error
    req.status = FeedbackRequest.Status.COMPLETED
    req.save(update_fields=['status'])
    return JsonResponse({'request': _feedback_request_to_dict(req)})


@require_POST
@admin_required
def reject_feedback_request(request, request_id):
    req, error = _get_feedback_or_404(request_id)
    if error:
        return error
    req.status = FeedbackRequest.Status.REJECTED
    req.save(update_fields=['status'])
    return JsonResponse({'request': _feedback_request_to_dict(req)})


@require_POST
@admin_required
def delete_feedback_request(request, request_id):
    req, error = _get_feedback_or_404(request_id)
    if error:
        return error
    if req.status != FeedbackRequest.Status.FAILED:
        return JsonResponse({'error': 'Only failed feedback requests can be deleted'}, status=400)
    req.delete()
    return JsonResponse({'deleted': True})

# Create your views here.
