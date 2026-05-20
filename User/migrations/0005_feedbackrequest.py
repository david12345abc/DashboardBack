from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('User', '0004_accessrequest'),
    ]

    operations = [
        migrations.CreateModel(
            name='FeedbackRequest',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('topic', models.CharField(choices=[('question', 'Вопрос по показателю'), ('bug', 'Сообщить об ошибке'), ('data', 'Проблема с данными'), ('feature', 'Предложение по улучшению')], default='question', max_length=32)),
                ('department', models.CharField(blank=True, max_length=255)),
                ('related_department', models.CharField(blank=True, max_length=255)),
                ('dashboard', models.CharField(blank=True, max_length=255)),
                ('period', models.CharField(blank=True, max_length=120)),
                ('description', models.TextField()),
                ('contact', models.CharField(blank=True, max_length=255)),
                ('attachment_names', models.JSONField(blank=True, default=list)),
                ('status', models.CharField(choices=[('new', 'Новое'), ('sent', 'Отправлено'), ('failed', 'Ошибка отправки')], default='new', max_length=20)),
                ('email_error', models.TextField(blank=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('user', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='feedback_requests', to='User.user')),
            ],
            options={
                'db_table': 'feedback_requests',
                'ordering': ['-created_at'],
            },
        ),
    ]
