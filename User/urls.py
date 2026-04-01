from django.urls import path

from . import views

urlpatterns = [
    path('login/', views.login, name='user-login'),
    path('register/', views.register, name='user-register'),
    path('me/', views.me, name='user-me'),
    path('list/', views.list_users, name='user-list'),
    path('<int:user_id>/', views.get_user, name='user-detail'),
    path('<int:user_id>/delete/', views.delete_user, name='user-delete'),
    path('<int:user_id>/edit/', views.edit_user, name='user-edit'),
]
