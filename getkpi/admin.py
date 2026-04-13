from django.contrib import admin

from .models import KpiDefinition


@admin.register(KpiDefinition)
class KpiDefinitionAdmin(admin.ModelAdmin):
    list_display = ('department', 'kpi_id', 'name', 'block', 'frequency', 'weight_pct')
    list_filter = ('department', 'block', 'frequency')
    search_fields = ('department', 'kpi_id', 'name')
    ordering = ('department', 'position')
