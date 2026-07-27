"""
Обновляет FND-T6: KPI = все проекты / проекты без отклонений по вехам × 100%.
"""
from django.db import migrations


DEPARTMENT = "Председатель совета директоров"
KPI_ID = "FND-T6"

NEW_FIELDS = {
    "name": "Портфель проектов",
    "goal": (
        "Контролировать проектный портфель: отношение всех проектов "
        "к проектам без отклонений по вехам"
    ),
    "formula": (
        "KPI = Количество всех проектов / Количество проектов "
        "без отклонений по вехам × 100%"
    ),
    "description": (
        "Все проекты портфеля за период; красным выделены проекты "
        "с отклонениями по вехам. KPI — все проекты, делённые на проекты "
        "без отклонений по вехам."
    ),
    "unit": "%",
    "source": "TurboProject / данные 1С",
    "monthly_target": "≤100%",
    "yearly_target": "≤100%",
    "green_threshold": "≤100%",
    "yellow_threshold": "100–110%",
    "red_threshold": ">110%",
}


def update_fnd_t6(apps, schema_editor):
    KpiDefinition = apps.get_model("getkpi", "KpiDefinition")
    KpiDefinition.objects.filter(
        department=DEPARTMENT, kpi_id=KPI_ID,
    ).update(**NEW_FIELDS)


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("getkpi", "0005_kpidefinition_description"),
    ]

    operations = [
        migrations.RunPython(update_fnd_t6, noop_reverse),
    ]
