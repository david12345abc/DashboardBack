from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('User', '0005_feedbackrequest'),
    ]

    operations = [
        migrations.AlterField(
            model_name='feedbackrequest',
            name='status',
            field=models.CharField(
                choices=[
                    ('new', 'Новое'),
                    ('sent', 'Отправлено'),
                    ('failed', 'Ошибка отправки'),
                    ('completed', 'Выполнено'),
                    ('rejected', 'Отклонено'),
                ],
                default='new',
                max_length=20,
            ),
        ),
    ]
