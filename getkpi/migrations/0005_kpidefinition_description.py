from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('getkpi', '0004_update_fnd_t6_portfolio'),
    ]

    operations = [
        migrations.AddField(
            model_name='kpidefinition',
            name='description',
            field=models.TextField(blank=True, null=True, verbose_name='Описание'),
        ),
    ]
