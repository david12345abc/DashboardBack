from django.urls import path

from . import views

urlpatterns = [
    path('', views.get_kpi, name='get-kpi'),
    path('all/', views.get_all_departments, name='get-all-kpi'),
    path('departments/', views.get_departments_list, name='departments-list'),
    path('structure/', views.get_structure, name='departments-structure'),
    path(
        'immediate-subordinates/',
        views.get_immediate_subordinates,
        name='immediate-subordinates',
    ),
    path('users/', views.get_users_departments, name='users-departments'),
    path('cache-status/', views.get_cache_status, name='cache-status'),
]
