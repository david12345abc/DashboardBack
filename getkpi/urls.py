from django.urls import path

from . import views

urlpatterns = [
    path('', views.get_kpi, name='get-kpi'),
    path('all/', views.get_all_departments, name='get-all-kpi'),
    path('departments/', views.get_departments_list, name='departments-list'),
]
