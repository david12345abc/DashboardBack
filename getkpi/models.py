from django.db import models


class KpiDefinition(models.Model):
    """Определение KPI — замена kpi_data.json."""

    department = models.CharField('Подразделение', max_length=255, db_index=True)
    kpi_id = models.CharField('Код KPI', max_length=50, db_index=True)
    name = models.CharField('Наименование', max_length=500)
    block = models.CharField('Блок дашборда', max_length=50, default='плитка')
    frequency = models.CharField('Периодичность', max_length=100, blank=True, default='')
    perspective = models.CharField('Перспектива', max_length=100, blank=True, default='')
    goal = models.TextField('Управленческая цель', blank=True, default='')
    formula = models.TextField('Формула расчёта', blank=True, default='')
    unit = models.CharField('Ед. изм.', max_length=50, blank=True, default='')
    source = models.CharField('Источник данных', max_length=500, blank=True, default='')
    monthly_target = models.CharField('Месячная цель', max_length=100, blank=True, null=True)
    quarterly_target = models.CharField('Квартальная цель', max_length=100, blank=True, null=True)
    yearly_target = models.CharField('Годовая цель', max_length=100, blank=True, null=True)
    green_threshold = models.CharField('Зелёный порог', max_length=100, blank=True, null=True)
    yellow_threshold = models.CharField('Жёлтый порог', max_length=100, blank=True, null=True)
    red_threshold = models.CharField('Красный порог', max_length=100, blank=True, null=True)
    weight_pct = models.FloatField('Вес, %', blank=True, null=True)
    chart_type = models.CharField('Тип графика', max_length=100, blank=True, null=True)
    chart_type_label = models.CharField('Подпись типа графика', max_length=200, blank=True, null=True)
    position = models.PositiveIntegerField('Порядок сортировки', default=0)

    class Meta:
        db_table = 'kpi_definition'
        ordering = ['department', 'position']
        unique_together = [('department', 'kpi_id')]
        verbose_name = 'KPI'
        verbose_name_plural = 'KPI-определения'

    def __str__(self):
        return f'{self.department} / {self.kpi_id} — {self.name}'

    def to_dict(self) -> dict:
        """Возвращает dict в том же формате, что был в kpi_data.json."""
        d = {
            'kpi_id': self.kpi_id,
            'name': self.name,
            'block': self.block,
            'frequency': self.frequency,
            'perspective': self.perspective,
            'goal': self.goal,
            'formula': self.formula,
            'unit': self.unit,
            'source': self.source,
            'monthly_target': self.monthly_target,
            'quarterly_target': self.quarterly_target,
            'yearly_target': self.yearly_target,
            'green_threshold': self.green_threshold,
            'yellow_threshold': self.yellow_threshold,
            'red_threshold': self.red_threshold,
            'weight_pct': self.weight_pct,
        }
        if self.chart_type:
            d['chart_type'] = self.chart_type
        if self.chart_type_label:
            d['chart_type_label'] = self.chart_type_label
        return d
