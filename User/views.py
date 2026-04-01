import json

from django.http import JsonResponse
from django.views.decorators.http import require_POST

from .models import User


@require_POST
def register(request):
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    nickname = data.get('nickname', '').strip()
    password = data.get('password', '')
    role = data.get('role', User.Role.USER5)

    if not nickname or not password:
        return JsonResponse({'error': 'nickname and password are required'}, status=400)

    if role not in User.Role.values:
        return JsonResponse({'error': f'Invalid role. Choose from: {User.Role.values}'}, status=400)

    if User.objects.filter(nickname=nickname).exists():
        return JsonResponse({'error': 'User with this nickname already exists'}, status=409)

    user = User(nickname=nickname, role=role)
    user.set_password(password)
    user.save()

    return JsonResponse({
        'id': user.id,
        'nickname': user.nickname,
        'role': user.role,
        'created_at': user.created_at.isoformat(),
    }, status=201)
