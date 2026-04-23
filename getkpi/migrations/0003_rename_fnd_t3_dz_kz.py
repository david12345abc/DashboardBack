"""
FND-T3 раньше был «ДДС 8 недель / отсутствие кассового разрыва»,
теперь используется под плитку «Соотношение ДЗ и КЗ» в ПСД «Мой дашборд».

Миграция обновляет поля существующей записи (name, formula, пороги, источник и т.п.)
прямо в БД, чтобы не пришлось ещё раз руками прогонять import_chairman_kpi.
"""
from django.db import migrations


DEPARTMENT = "Председатель совета директоров"
KPI_ID = "FND-T3"

NEW_FIELDS = {
    "name": "Соотношение ДЗ и КЗ",
    "block": "плитка",
    "frequency": "Ежемесячно",
    "perspective": "Финансы",
    "goal": "Видеть баланс дебиторской и кредиторской задолженности по клиентам и поставщикам",
    "formula": (
        "ДЗ / КЗ × 100% — отдельно для клиентов "
        "(РасчетыСКлиентамиПоСрокам) и поставщиков "
        "(РасчетыСПоставщикамиПоДокументам: ПредоплатаРегл / ДолгРегл)"
    ),
    "unit": "%",
    "source": (
        "1С: РасчетыСКлиентамиПоСрокам, "
        "РасчетыСПоставщикамиПоДокументам (регл. валюта)"
    ),
    "monthly_target": "≥100%",
    "yearly_target": "≥100%",
    "green_threshold": "≥100%",
    "yellow_threshold": "80–99,9%",
    "red_threshold": "<80%",
    "weight_pct": 10.0,
}


def rename_fnd_t3(apps, schema_editor):
    KpiDefinition = apps.get_model("getkpi", "KpiDefinition")
    KpiDefinition.objects.filter(
        department=DEPARTMENT, kpi_id=KPI_ID,
    ).update(**NEW_FIELDS)


def noop_reverse(apps, schema_editor):
    # Автоматический откат не делаем — старые значения нам не требуются.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("getkpi", "0002_remove_chairman_obsolete_kpis"),
    ]

    operations = [
        migrations.RunPython(rename_fnd_t3, noop_reverse),
    ]
