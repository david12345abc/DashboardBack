from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('User', '0004_accessrequest'),
    ]

    operations = [
        migrations.CreateModel(
            name='UserTileOrder',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('scope_key', models.CharField(max_length=400)),
                ('order', models.JSONField(default=list)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='tile_orders', to='User.user')),
            ],
            options={
                'db_table': 'user_tile_orders',
                'constraints': [
                    models.UniqueConstraint(fields=('user', 'scope_key'), name='user_tile_order_scope'),
                ],
            },
        ),
    ]
