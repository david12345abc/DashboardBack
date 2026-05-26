from django.urls import path

from . import views


urlpatterns = [
    path('chat/', views.chat, name='aiassistant-chat'),
    path('jobs/<str:job_id>/', views.job_detail, name='aiassistant-job-detail'),
    path('jobs/<str:job_id>/stream/', views.job_stream, name='aiassistant-job-stream'),
    path('jobs/<str:job_id>/stop/', views.job_stop, name='aiassistant-job-stop'),
]
