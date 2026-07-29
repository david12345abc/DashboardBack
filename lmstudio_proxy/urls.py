from django.urls import path

from . import views


urlpatterns = [
    path("health/", views.health, name="lmstudio-health"),
    path("v1/models/", views.models, name="lmstudio-models"),
    path(
        "v1/chat/completions/",
        views.chat_completions,
        name="lmstudio-chat-completions",
    ),
]
