import json
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path

import jwt
from django.conf import settings
from django.core.mail import EmailMessage
from django.http import JsonResponse
from django.utils import timezone as django_timezone
from django.views.decorators.http import require_POST, require_GET, require_http_methods

from .models import AccessRequest, User


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

def _generate_token(user: User) -> str:
    payload = {
        'user_id': user.id,
        'nickname': user.nickname,
        'role': user.role,
        'exp': datetime.now(timezone.utc) + timedelta(days=settings.JWT_EXPIRATION_DAYS),
        'iat': datetime.now(timezone.utc),
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


def _decode_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None


def _get_current_user(request) -> User | None:
    auth = request.headers.get('Authorization', '')
    if not auth.startswith('Bearer '):
        return None
    payload = _decode_token(auth[7:])
    if payload is None:
        return None
    try:
        return User.objects.get(id=payload['user_id'])
    except User.DoesNotExist:
        return None


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

def login_required(fn):
    @wraps(fn)
    def wrapper(request, *args, **kwargs):
        user = _get_current_user(request)
        if user is None:
            return JsonResponse({'error': 'Authentication required'}, status=401)
        request.current_user = user
        return fn(request, *args, **kwargs)
    return wrapper


def admin_required(fn):
    @wraps(fn)
    @login_required
    def wrapper(request, *args, **kwargs):
        if not request.current_user.is_admin:
            return JsonResponse({'error': 'Admin (User1) privileges required'}, status=403)
        return fn(request, *args, **kwargs)
    return wrapper


def _user_to_dict(user: User) -> dict:
    return {
        'id': user.id,
        'nickname': user.nickname,
        'role': user.role,
        'department': user.department,
        'created_at': user.created_at.isoformat(),
    }


def _access_request_to_dict(req: AccessRequest) -> dict:
    return {
        'id': req.id,
        'request_type': req.request_type,
        'request_type_label': req.get_request_type_display(),
        'status': req.status,
        'status_label': req.get_status_display(),
        'nickname': req.nickname,
        'department': req.department,
        'created_at': req.created_at.isoformat(),
        'processed_at': req.processed_at.isoformat() if req.processed_at else None,
        'processed_by': req.processed_by.nickname if req.processed_by else None,
        'comment': req.comment,
    }


def _parse_json_body(request) -> tuple[dict | None, JsonResponse | None]:
    try:
        return json.loads(request.body or b'{}'), None
    except json.JSONDecodeError:
        return None, JsonResponse({'error': 'Invalid JSON'}, status=400)


def _flatten_structure_departments(node, out: list[str]) -> None:
    if not isinstance(node, dict):
        return
    for name, children in node.items():
        if isinstance(name, str) and name.strip():
            out.append(name.strip())
        _flatten_structure_departments(children, out)


def _load_departments() -> list[str]:
    path = Path(settings.BASE_DIR) / 'getkpi' / 'structure.json'
    try:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []
    departments: list[str] = []
    _flatten_structure_departments(data, departments)
    seen: set[str] = set()
    unique: list[str] = []
    for dep in departments:
        key = dep.casefold()
        if key in seen:
            continue
        seen.add(key)
        unique.append(dep)
    return unique


def _validate_department(department: str) -> bool:
    return department in set(_load_departments())


def _send_access_request_email(req: AccessRequest) -> None:
    subject = f'[Дашборд] Новая заявка: {req.get_request_type_display()}'
    body = '\n'.join([
        f'Тип заявки: {req.get_request_type_display()}',
        f'Логин: {req.nickname}',
        f'Подразделение: {req.department or "—"}',
        f'Статус: {req.get_status_display()}',
        f'Создана: {req.created_at.isoformat()}',
        '',
        'Заявка также доступна в админ-панели дашборда.',
    ])
    message = EmailMessage(
        subject=subject,
        body=body,
        from_email=settings.DEFAULT_FROM_EMAIL,
        to=[settings.FEEDBACK_EMAIL_TO],
    )
    message.send(fail_silently=False)


# ---------------------------------------------------------------------------
# POST /api/user/login/
# ---------------------------------------------------------------------------

@require_POST
def login(request):
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    nickname = data.get('nickname', '').strip()
    password = data.get('password', '')

    if not nickname or not password:
        return JsonResponse({'error': 'nickname and password are required'}, status=400)

    try:
        user = User.objects.get(nickname=nickname)
    except User.DoesNotExist:
        return JsonResponse({'error': 'Invalid nickname or password'}, status=401)

    if not user.verify_password(password):
        return JsonResponse({'error': 'Invalid nickname or password'}, status=401)

    token = _generate_token(user)
    return JsonResponse({
        'token': token,
        'user': _user_to_dict(user),
    })


@require_GET
def departments(request):
    deps = _load_departments()
    return JsonResponse({'departments': deps, 'count': len(deps)})


@require_POST
def request_registration(request):
    data, error = _parse_json_body(request)
    if error:
        return error

    nickname = data.get('nickname', '').strip()
    password = data.get('password', '')
    department = data.get('department', '').strip()

    if not nickname or not password or not department:
        return JsonResponse({'error': 'nickname, password and department are required'}, status=400)
    if len(password) < 6:
        return JsonResponse({'error': 'password must contain at least 6 characters'}, status=400)
    if not _validate_department(department):
        return JsonResponse({'error': 'Unknown department'}, status=400)
    if User.objects.filter(nickname=nickname).exists():
        return JsonResponse({'error': 'User with this nickname already exists'}, status=409)
    if AccessRequest.objects.filter(
        nickname=nickname,
        request_type=AccessRequest.RequestType.REGISTRATION,
        status=AccessRequest.Status.PENDING,
    ).exists():
        return JsonResponse({'error': 'Registration request is already pending'}, status=409)

    req = AccessRequest(
        request_type=AccessRequest.RequestType.REGISTRATION,
        nickname=nickname,
        department=department,
    )
    req.set_password(password)
    req.save()
    email_sent = True
    try:
        _send_access_request_email(req)
    except Exception:
        email_sent = False
    return JsonResponse({'request': _access_request_to_dict(req), 'email_sent': email_sent}, status=201)


@require_POST
def request_password_reset(request):
    data, error = _parse_json_body(request)
    if error:
        return error

    nickname = data.get('nickname', '').strip()
    password = data.get('password', '')
    if not nickname or not password:
        return JsonResponse({'error': 'nickname and password are required'}, status=400)
    if len(password) < 6:
        return JsonResponse({'error': 'password must contain at least 6 characters'}, status=400)
    try:
        user = User.objects.get(nickname=nickname)
    except User.DoesNotExist:
        return JsonResponse({'error': 'User not found'}, status=404)
    if AccessRequest.objects.filter(
        nickname=nickname,
        request_type=AccessRequest.RequestType.PASSWORD_RESET,
        status=AccessRequest.Status.PENDING,
    ).exists():
        return JsonResponse({'error': 'Password reset request is already pending'}, status=409)

    req = AccessRequest(
        request_type=AccessRequest.RequestType.PASSWORD_RESET,
        nickname=nickname,
        department=user.department,
    )
    req.set_password(password)
    req.save()
    return JsonResponse({'request': _access_request_to_dict(req)}, status=201)


# ---------------------------------------------------------------------------
# POST /api/user/register/    (только User1)
# ---------------------------------------------------------------------------

@require_POST
@admin_required
def register(request):
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    nickname = data.get('nickname', '').strip()
    password = data.get('password', '')
    role = data.get('role', User.Role.USER5)
    department = data.get('department', '').strip()

    if not nickname or not password:
        return JsonResponse({'error': 'nickname and password are required'}, status=400)

    if not department:
        return JsonResponse({'error': 'department is required'}, status=400)

    if role not in User.Role.values:
        return JsonResponse({'error': f'Invalid role. Choose from: {User.Role.values}'}, status=400)

    if User.objects.filter(nickname=nickname).exists():
        return JsonResponse({'error': 'User with this nickname already exists'}, status=409)

    user = User(nickname=nickname, role=role, department=department)
    user.set_password(password)
    user.save()

    return JsonResponse(_user_to_dict(user), status=201)


# ---------------------------------------------------------------------------
# GET /api/user/<id>/         (только User1)
# ---------------------------------------------------------------------------

@require_GET
@admin_required
def get_user(request, user_id):
    try:
        user = User.objects.get(id=user_id)
    except User.DoesNotExist:
        return JsonResponse({'error': 'User not found'}, status=404)

    return JsonResponse(_user_to_dict(user))


# ---------------------------------------------------------------------------
# GET /api/user/list/         (только User1)
# ---------------------------------------------------------------------------

@require_GET
@admin_required
def list_users(request):
    users = User.objects.all().order_by('id')
    return JsonResponse({'users': [_user_to_dict(u) for u in users]})


@require_GET
@admin_required
def list_access_requests(request):
    status = request.GET.get('status', '').strip()
    qs = AccessRequest.objects.all()
    if status:
        if status not in AccessRequest.Status.values:
            return JsonResponse({'error': 'Invalid status'}, status=400)
        qs = qs.filter(status=status)
    return JsonResponse({'requests': [_access_request_to_dict(req) for req in qs]})


@require_POST
@admin_required
def approve_access_request(request, request_id):
    try:
        req = AccessRequest.objects.get(id=request_id)
    except AccessRequest.DoesNotExist:
        return JsonResponse({'error': 'Request not found'}, status=404)
    if req.status != AccessRequest.Status.PENDING:
        return JsonResponse({'error': 'Request is already processed'}, status=400)

    if req.request_type == AccessRequest.RequestType.REGISTRATION:
        if User.objects.filter(nickname=req.nickname).exists():
            return JsonResponse({'error': 'User with this nickname already exists'}, status=409)
        user = User(nickname=req.nickname, password=req.password_hash, role=User.Role.USER5, department=req.department)
        user.save()
    elif req.request_type == AccessRequest.RequestType.PASSWORD_RESET:
        try:
            user = User.objects.get(nickname=req.nickname)
        except User.DoesNotExist:
            return JsonResponse({'error': 'User not found'}, status=404)
        user.password = req.password_hash
        user.save(update_fields=['password'])
    else:
        return JsonResponse({'error': 'Unknown request type'}, status=400)

    req.status = AccessRequest.Status.APPROVED
    req.processed_at = django_timezone.now()
    req.processed_by = request.current_user
    req.save(update_fields=['status', 'processed_at', 'processed_by'])
    return JsonResponse({'request': _access_request_to_dict(req)})


@require_POST
@admin_required
def reject_access_request(request, request_id):
    try:
        req = AccessRequest.objects.get(id=request_id)
    except AccessRequest.DoesNotExist:
        return JsonResponse({'error': 'Request not found'}, status=404)
    if req.status != AccessRequest.Status.PENDING:
        return JsonResponse({'error': 'Request is already processed'}, status=400)
    data, error = _parse_json_body(request)
    if error:
        return error
    req.status = AccessRequest.Status.REJECTED
    req.processed_at = django_timezone.now()
    req.processed_by = request.current_user
    req.comment = (data.get('comment') or '').strip()
    req.save(update_fields=['status', 'processed_at', 'processed_by', 'comment'])
    return JsonResponse({'request': _access_request_to_dict(req)})


# ---------------------------------------------------------------------------
# DELETE /api/user/<id>/delete/   (только User1)
# ---------------------------------------------------------------------------

@require_POST
@admin_required
def delete_user(request, user_id):
    try:
        user = User.objects.get(id=user_id)
    except User.DoesNotExist:
        return JsonResponse({'error': 'User not found'}, status=404)

    if user.id == request.current_user.id:
        return JsonResponse({'error': 'Cannot delete yourself'}, status=400)

    user.delete()
    return JsonResponse({'message': 'User deleted'})


# ---------------------------------------------------------------------------
# PUT /api/user/<id>/edit/
# Сам себя — любой авторизованный; чужого — только User1
# ---------------------------------------------------------------------------

@require_http_methods(['PUT', 'PATCH'])
@login_required
def edit_user(request, user_id):
    try:
        target = User.objects.get(id=user_id)
    except User.DoesNotExist:
        return JsonResponse({'error': 'User not found'}, status=404)

    is_self = request.current_user.id == target.id
    is_admin = request.current_user.is_admin

    if not is_self and not is_admin:
        return JsonResponse({'error': 'You can only edit yourself or be User1'}, status=403)

    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    if 'nickname' in data:
        new_nickname = data['nickname'].strip()
        if not new_nickname:
            return JsonResponse({'error': 'nickname cannot be empty'}, status=400)
        if User.objects.filter(nickname=new_nickname).exclude(id=target.id).exists():
            return JsonResponse({'error': 'This nickname is already taken'}, status=409)
        target.nickname = new_nickname

    if 'password' in data:
        if not data['password']:
            return JsonResponse({'error': 'password cannot be empty'}, status=400)
        target.set_password(data['password'])

    if 'role' in data:
        if not is_admin:
            return JsonResponse({'error': 'Only User1 can change roles'}, status=403)
        if data['role'] not in User.Role.values:
            return JsonResponse({'error': f'Invalid role. Choose from: {User.Role.values}'}, status=400)
        target.role = data['role']

    if 'department' in data:
        target.department = data['department'].strip()

    target.save()
    return JsonResponse(_user_to_dict(target))


# ---------------------------------------------------------------------------
# GET /api/user/me/           (любой авторизованный)
# ---------------------------------------------------------------------------

@require_GET
@login_required
def me(request):
    return JsonResponse(_user_to_dict(request.current_user))
