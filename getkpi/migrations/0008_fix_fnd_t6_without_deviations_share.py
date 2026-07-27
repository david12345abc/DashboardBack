"""
Обновляет FND-T6: KPI = проекты без отклонений по вехам / все проекты × 100%.
"""
from django.db import migrations


DEPARTMENT = "Председатель совета директоров"
KPI_ID = "FND-T6"

NEW_FIELDS = {
    "name": "Портфель проектов",
    "goal": (
        "Контролировать проектный портфель: долю проектов "
        "без отклонений по вехам"
    ),
    "formula": (
        "KPI = (Количество всех проектов - проекты с отклонениями по вехам) / "
        "Количество всех проектов × 100%"
    ),
    "description": (
        "Все проекты портфеля за период; красным выделены проекты "
        "с отклонениями по вехам. KPI — проекты без отклонений по вехам, "
        "делённые на все проекты (выше — лучше)."
    ),
    "unit": "%",
    "source": "TurboProject / данные 1С",
    "monthly_target": "≥95%",
    "yearly_target": "≥95%",
    "green_threshold": "≥95%",
    "yellow_threshold": "85–94,9%",
    "red_threshold": "<85%",
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
        ("getkpi", "0007_revert_fnd_t6_deviation_share"),
    ]

    operations = [
        migrations.RunPython(update_fnd_t6, noop_reverse),
    ]
