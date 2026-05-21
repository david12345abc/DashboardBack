# dept_protocol

Инструменты для сопоставления департаментов проекта с оргструктурой 1С и получения просроченных задач протоколов по руководителю.

ФИО сотрудников **не хранятся в JSON** — каждый раз читаются из актуальных кадровых данных 1С.

## Файлы

### Данные

| Файл | Назначение |
|---|---|
| `source_department_holders_compact.json` | Справочник сопоставлений: `source` (название в проекте) → `matched_1c` (путь в оргструктуре 1С) |
| `enterprise_positions_report_all.txt` | Кэш кадровой выгрузки (оргструктура, должности, ФИО). Обновляется через `list_enterprise_positions.py` |

### Скрипты

| Файл | Назначение |
|---|---|
| `list_enterprise_positions.py` | Выгружает из 1С всю оргструктуру с должностями и ФИО в txt-файл |
| `lookup_source_department.py` | По названию департамента (`source`) показывает путь в 1С и сотрудников на должностях |
| `lookup_user_ref.py` | По ФИО находит `Ref_Key` (GUID) пользователя в `Catalog_Пользователи` |
| `protocol_tasks_by_leader.py` | Просроченные незавершённые задачи протоколов по ФИО руководителя |
| `protocol_tasks_by_department.py` | **Главный скрипт**: департамент → ФИО из 1С → Ref_Key → просроченные задачи |

### Внутренний модуль

| Файл | Назначение |
|---|---|
| `department_holders.py` | Логика поиска ФИО на должности по `source` и пути `matched_1c`. Используется `protocol_tasks_by_department.py`, отдельно не запускается |

## Как работает `protocol_tasks_by_department.py`

```
Департамент (source)
    ↓  source_department_holders_compact.json
matched_1c (путь в оргструктуре)
    ↓  кадровые данные 1С (department_holders.py)
ФИО на должности
    ↓  Catalog_Пользователи (lookup_user_ref.py)
Ref_Key
    ↓  InformationRegister_ТД_ЗадачиПротоколов
Просроченные задачи протоколов
```

Критерий просрочки (как в 1С):

- задача не выполнена (`ДатаИсполнения` пустая)
- `СрокИсполнения` **строго меньше** текущей даты

## Примеры запуска

```bash
cd dept_protocol

# Список всех департаментов из JSON
python protocol_tasks_by_department.py --list

# Просроченные задачи по департаменту
python protocol_tasks_by_department.py "Технический директор"
python protocol_tasks_by_department.py -i

# Другие форматы вывода
python protocol_tasks_by_department.py --table "Отдел ВЭД"
python protocol_tasks_by_department.py --csv tasks.csv "ОДП"
python protocol_tasks_by_department.py --date 2026-05-19 "Технический директор"

# Просмотр сотрудников департамента
python lookup_source_department.py "Технический директор"
python lookup_source_department.py --list

# Ref_Key по ФИО
python lookup_user_ref.py "Улановский Константин Владимирович"

# Задачи напрямую по ФИО
python protocol_tasks_by_leader.py "Улановский Константин Владимирович"

# Обновить кэш оргструктуры (опционально)
python list_enterprise_positions.py
```

## Подключение к 1С

По умолчанию:

- OData: `http://192.168.2.229:81/erp_pm/odata/standard.odata`
- Пользователь: `odata.user`

Переменные окружения: `ONEC_BASE_URL`, `ODATA_USER`, `ODATA_PASSWORD`

## Редактирование справочника департаментов

Чтобы добавить или изменить сопоставление, отредактируйте `source_department_holders_compact.json`:

```json
{
  "departments": [
    {
      "source": "Технический директор",
      "matched_1c": "Председатель Совета Директоров / ОПЕРАЦИОННЫЙ ДИРЕКТОР / ТЕХНИЧЕСКИЙ ДИРЕКТОР"
    }
  ]
}
```

Путь `matched_1c` можно уточнить по выгрузке `list_enterprise_positions.py` или файлу `enterprise_positions_report_all.txt`.
