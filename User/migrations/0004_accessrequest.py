from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('User', '0003_alter_user_department'),
    ]

    operations = [
        migrations.CreateModel(
            name='AccessRequest',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('request_type', models.CharField(choices=[('registration', 'Регистрация'), ('password_reset', 'Сброс пароля')], max_length=32)),
                ('status', models.CharField(choices=[('pending', 'Ожидает'), ('approved', 'Одобрена'), ('rejected', 'Отклонена')], default='pending', max_length=20)),
                ('nickname', models.CharField(max_length=150)),
                ('password_hash', models.CharField(max_length=256)),
                ('department', models.CharField(blank=True, max_length=255)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('processed_at', models.DateTimeField(blank=True, null=True)),
                ('comment', models.TextField(blank=True)),
                ('processed_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='processed_access_requests', to='User.user')),
            ],
            options={
                'db_table': 'access_requests',
                'ordering': ['-created_at'],
            },
        ),
    ]
