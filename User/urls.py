from django.urls import path

from . import views

urlpatterns = [
    path('login/', views.login, name='user-login'),
    path('departments/', views.departments, name='user-departments'),
    path('access-requests/register/', views.request_registration, name='access-request-registration'),
    path('access-requests/password-reset/', views.request_password_reset, name='access-request-password-reset'),
    path('access-requests/', views.list_access_requests, name='access-request-list'),
    path('access-requests/<int:request_id>/approve/', views.approve_access_request, name='access-request-approve'),
    path('access-requests/<int:request_id>/reject/', views.reject_access_request, name='access-request-reject'),
    path('register/', views.register, name='user-register'),
    path('me/', views.me, name='user-me'),
    path('list/', views.list_users, name='user-list'),
    path('<int:user_id>/', views.get_user, name='user-detail'),
    path('<int:user_id>/delete/', views.delete_user, name='user-delete'),
    path('<int:user_id>/edit/', views.edit_user, name='user-edit'),
]
