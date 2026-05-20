"""
URL configuration for Dashbord project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include

from User import views as user_views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/user/', include('User.urls')),
    path('api/feedback-requests/', user_views.feedback_requests, name='feedback-request-list-create-alt'),
    path('api/feedback-requests/admin/', user_views.list_feedback_requests, name='feedback-request-admin-list-alt'),
    path('api/feedback-requests/<int:request_id>/complete/', user_views.complete_feedback_request, name='feedback-request-complete-alt'),
    path('api/feedback-requests/<int:request_id>/reject/', user_views.reject_feedback_request, name='feedback-request-reject-alt'),
    path('api/feedback-requests/<int:request_id>/delete/', user_views.delete_feedback_request, name='feedback-request-delete-alt'),
    path('api/kpi/', include('getkpi.urls')),
    path('api/search/', include('searchengine.urls')),
]
