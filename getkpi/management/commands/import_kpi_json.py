"""
Импорт kpi_data.json → таблицу kpi_definition (модель KpiDefinition).

Использование:
    py manage.py import_kpi_json                 # из стандартного расположения
    py manage.py import_kpi_json --file path.json # из произвольного файла
    py manage.py import_kpi_json --clear          # предварительно очистить таблицу
"""
import json
from pathlib import Path

from django.core.management.base import BaseCommand

from getkpi.models import KpiDefinition

FIELDS = (
    'kpi_id', 'name', 'block', 'frequency', 'perspective', 'goal',
    'formula', 'unit', 'source',
    'monthly_target', 'quarterly_target', 'yearly_target',
    'green_threshold', 'yellow_threshold', 'red_threshold',
    'weight_pct', 'chart_type', 'chart_type_label',
)


class Command(BaseCommand):
    help = 'Импорт KPI из kpi_data.json в PostgreSQL'

    def add_arguments(self, parser):
        parser.add_argument(
            '--file', type=str, default=None,
            help='Путь к JSON-файлу (по умолчанию: getkpi/kpi_data.json)',
        )
        parser.add_argument(
            '--clear', action='store_true',
            help='Очистить таблицу перед импортом',
        )

    def handle(self, *args, **options):
        json_path = options['file']
        if not json_path:
            json_path = Path(__file__).resolve().parents[2] / 'kpi_data.json'
        else:
            json_path = Path(json_path)

        if not json_path.exists():
            self.stderr.write(self.style.ERROR(f'Файл не найден: {json_path}'))
            return

        with open(json_path, encoding='utf-8') as f:
            data: dict[str, list[dict]] = json.load(f)

        if options['clear']:
            deleted, _ = KpiDefinition.objects.all().delete()
            self.stdout.write(f'Удалено {deleted} записей')

        created = 0
        updated = 0
        for department, kpi_list in data.items():
            for pos, kpi in enumerate(kpi_list):
                defaults = {'position': pos}
                for field in FIELDS:
                    if field == 'kpi_id':
                        continue
                    val = kpi.get(field)
                    if val is not None:
                        defaults[field] = val

                _, is_new = KpiDefinition.objects.update_or_create(
                    department=department,
                    kpi_id=kpi.get('kpi_id', f'UNKNOWN-{pos}'),
                    defaults=defaults,
                )
                if is_new:
                    created += 1
                else:
                    updated += 1

        total = created + updated
        self.stdout.write(self.style.SUCCESS(
            f'Импорт завершён: {total} записей ({created} новых, {updated} обновлённых)'
        ))
