from django.contrib import admin

from .models import FeedbackRequest


@admin.register(FeedbackRequest)
class FeedbackRequestAdmin(admin.ModelAdmin):
    list_display = ('id', 'topic', 'status', 'user', 'department', 'related_department', 'created_at')
    list_filter = ('topic', 'status', 'created_at')
    search_fields = ('description', 'contact', 'department', 'related_department', 'user__nickname')

# Register your models here.
