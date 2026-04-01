import json
from datetime import datetime, timedelta, timezone
from functools import wraps

import jwt
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.http import require_POST, require_GET, require_http_methods

from .models import User


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
