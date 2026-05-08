# Слияние `origin/new_kukuagu` в текущую ветку

**Дата:** 2026-05-08  

## Конфликты при `git merge origin/new_kukuagu`

| Файл | Суть | Как разрешено |
|------|------|---------------|
| `getkpi/cache_manager.py` | HEAD: задачи прогрева для зама по производству, логистики; ветка: `devdir` RD-M1/M3/M4 | Оба набора задач и оба импорта объединены в `_build_warm_tasks`. |
| `getkpi/views.py` | Техдир: локальная `_build_techdir_charts` (HEAD) vs `techdir_dashboard.build_charts` (ветка); `ref` для месяца; блок generic-таблиц | Для техдира вызывается `techdir_dashboard.build_charts`. Сохранены `_line_values_from_points`, `_build_prod_deputy_charts`, графики логистики и зама по производству. В условии `ref_y/ref_m` объединены ветвь devdir и HEAD (главный конструктор, логистика). Таблицы: как в HEAD, с `techdir_dashboard.is_techdir_department` (не `_is_techdir_department`). Локальная `_build_techdir_charts` удалена как дубликат `techdir_dashboard.build_charts`. |
