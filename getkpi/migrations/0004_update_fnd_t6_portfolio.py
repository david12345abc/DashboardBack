"""
Обновляет FND-T6 под реальный расчёт «Портфель проектов».
"""
from django.db import migrations


DEPARTMENT = "Председатель совета директоров"
KPI_ID = "FND-T6"

NEW_FIELDS = {
    "name": "Портфель проектов",
    "goal": "Видеть объём проектного портфеля по вехам и долю проектов с отклонением по baseline",
    "formula": "Количество проектов с вехами в месяце и отклонения по ним; KPI = Отклонения / Портфель × 100%",
    "unit": "%",
    "source": "TurboProject / данные 1С",
    "monthly_target": "≤100%",
    "yearly_target": "≤100%",
    "green_threshold": "<100%",
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
        ("getkpi", "0003_rename_fnd_t3_dz_kz"),
    ]

    operations = [
        migrations.RunPython(update_fnd_t6, noop_reverse),
    ]
