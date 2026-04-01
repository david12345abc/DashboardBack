from django.contrib import admin

from .models import User


@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    list_display = ('id', 'nickname', 'role', 'created_at')
    list_filter = ('role',)
    search_fields = ('nickname',)
