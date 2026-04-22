from django.db import migrations


DEPARTMENT = "Председатель совета директоров"
KPI_IDS_TO_REMOVE = ("FND-T8", "FND-T10")


def remove_chairman_obsolete_kpis(apps, schema_editor):
    KpiDefinition = apps.get_model("getkpi", "KpiDefinition")
    KpiDefinition.objects.filter(
        department=DEPARTMENT, kpi_id__in=KPI_IDS_TO_REMOVE,
    ).delete()


def noop_reverse(apps, schema_editor):
    # Откатить удаление мы не сможем автоматически (запись в БД больше не нужна),
    # поэтому обратная миграция — пустая.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("getkpi", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(remove_chairman_obsolete_kpis, noop_reverse),
    ]
