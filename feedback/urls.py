from django.urls import path

from . import views

urlpatterns = [
    path('support/', views.guide_support_request, name='guide-support-request'),
    path('', views.feedback_requests, name='feedback-request-list-create'),
    path('admin/', views.list_feedback_requests, name='feedback-request-admin-list'),
    path('<int:request_id>/complete/', views.complete_feedback_request, name='feedback-request-complete'),
    path('<int:request_id>/reject/', views.reject_feedback_request, name='feedback-request-reject'),
    path('<int:request_id>/delete/', views.delete_feedback_request, name='feedback-request-delete'),
]
