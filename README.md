# Dashbord Backend

Бэкенд на Django 6 + PostgreSQL.

---

## Структура проекта

```
Dashbord/
├── manage.py
├── package.json                # Конфигурация (БД, секретный ключ)
├── requirements.txt
├── Dashbord/                   # Главный модуль Django
│   ├── settings.py
│   ├── urls.py
├── User/                       # Приложение «Пользователи»
│   ├── models.py / views.py / urls.py / admin.py
│   └── management/commands/createsuperadmin.py
└── getkpi/                     # Приложение «KPI подразделений»
    ├── views.py / urls.py
    ├── valovaya_pribyl.py      # Расчёт ВП из кэшей 1С
    ├── kpi_data.json           # Справочник KPI (28 подразделений)
    ├── structure.json          # Иерархия подразделений (права доступа)
    └── dashboard/
        └── ВаловаяПрибыль/     # Скрипты загрузки данных из 1С OData
            ├── step1_scan.py   # Скан регистра ВыручкаИСебестоимостьПродаж
            ├── fetch_costs.py  # Загрузка unit-cost из СтоимостьТоваров
            └── step2_calc.py   # Финальный расчёт ВП
```

---

## Запуск

```bash
python -m venv venv
venv\Scripts\Activate
pip install -r requirements.txt
# заполнить package.json (БД, секрет)
python manage.py migrate
python manage.py createsuperadmin --nickname admin --password yourpassword
python manage.py runserver
```

---

## Аутентификация

Все защищённые эндпоинты требуют заголовок:

```
Authorization: Bearer <token>
```

Токен выдаётся при логине. Срок жизни — 7 дней.

---

## API — User (`/api/user/`)

### 1. `POST /api/user/login/` — Логин

**Доступ:** все (без токена)
**Header:** `Content-Type: application/json`
**Body:** `{"nickname": "admin", "password": "secret123"}`

**Ответ 200:**

```json
{
  "token": "eyJhbGci...",
  "user": {"id": 1, "nickname": "admin", "role": "User1", "department": "коммерческий директор", "created_at": "..."}
}
```

| Код | Причина |
|-----|---------|
| 400 | Не передан nickname или password |
| 401 | Неверный логин или пароль |

---

### 2. `POST /api/user/register/` — Регистрация

**Доступ:** только User1
**Header:** `Content-Type: application/json`, `Authorization: Bearer <token>`
**Body:**

```json
{"nickname": "john", "password": "pass123", "role": "User3", "department": "Коммерческая служба"}
```

| Поле | Обяз. | Описание |
|------|-------|----------|
| nickname | да | Уникальный логин |
| password | да | Пароль |
| role | нет | User1–User5 (по умолчанию User5) |
| department | да | Подразделение |

**Ответ 201:** `{"id": 2, "nickname": "john", "role": "User3", "department": "...", "created_at": "..."}`

| Код | Причина |
|-----|---------|
| 400 | Не передано обязательное поле / невалидная роль |
| 403 | Не User1 |
| 409 | nickname занят |

---

### 3. `GET /api/user/me/` — Информация о себе

**Доступ:** любой авторизованный
**Header:** `Authorization: Bearer <token>`
**Body:** нет

**Ответ 200:** `{"id": 1, "nickname": "admin", "role": "User1", "department": "...", "created_at": "..."}`

---

### 4. `GET /api/user/<id>/` — Пользователь по ID

**Доступ:** только User1
**Header:** `Authorization: Bearer <token>`
**Body:** нет
**Ответ 200:** `{"id": 2, "nickname": "john", "role": "User3", "department": "...", "created_at": "..."}`
**Ошибки:** 403 не User1, 404 не найден

---

### 5. `GET /api/user/list/` — Список пользователей

**Доступ:** только User1
**Header:** `Authorization: Bearer <token>`
**Body:** нет
**Ответ 200:** `{"users": [{"id": 1, "nickname": "admin", ...}, ...]}`

---

### 6. `POST /api/user/<id>/delete/` — Удаление

**Доступ:** только User1
**Header:** `Authorization: Bearer <token>`
**Body:** нет
**Ответ 200:** `{"message": "User deleted"}`
**Ошибки:** 400 нельзя удалить себя, 403 не User1, 404 не найден

---

### 7. `PUT|PATCH /api/user/<id>/edit/` — Редактирование

**Доступ:** себя — любой авторизованный; чужого — только User1
**Header:** `Content-Type: application/json`, `Authorization: Bearer <token>`
**Body** (всё опционально): `{"nickname": "...", "password": "...", "role": "User2", "department": "..."}`

> Роль может менять только User1.

**Ответ 200:** `{"id": 2, "nickname": "...", "role": "...", "department": "...", "created_at": "..."}`
**Ошибки:** 400 пустое поле, 403 нет прав, 404 не найден, 409 nickname занят

---

## API — KPI (`/api/kpi/`)

Все эндпоинты: `Authorization: Bearer <token>`, Body: нет.

Права доступа определяются по `structure.json`: пользователь видит KPI **своего** подразделения и **всех подчинённых** по дереву иерархии.

---

### 8. `GET /api/kpi/` — KPI подразделения

Возвращает KPI подразделения пользователя. Через `?department=` можно запросить KPI подчинённого подразделения.

| Query-параметр | Описание |
|----------------|----------|
| `department` | Подразделение (по умолчанию — своё). Должно быть в ветке иерархии. |

**Ответ 200:**

```json
{
  "department": "Коммерческий директор",
  "kpi_count": 12,
  "kpis": [
    {
      "kpi_id": "KD-M1",
      "name": "Валовая прибыль коммерческого блока факт/план",
      "block": "плитка",
      "frequency": "ежемесячно",
      "value": 354.5,
      "valovaya_pribyl": {
        "year": 2026,
        "plan_monthly": 28450241,
        "months": [
          {"month": 3, "plan": 28450241, "fact": 100847809.68, "kpi_pct": 354.5, "has_data": true, "vyruchka": 136503177.67, "sebestoimost": 35655367.99}
        ],
        "ytd": {"total_plan": 28450241, "total_fact": 100847809.68, "kpi_pct": 354.5, "months_with_data": 1}
      },
      "formula": "Факт валовой прибыли / План × 100%",
      "unit": "%",
      "weight_pct": 50,
      "green_threshold": "≥100%",
      "yellow_threshold": "90–99,9%",
      "red_threshold": "<90%"
    }
  ]
}
```

> **KD-M1 / KD-Y1 (Валовая прибыль):** данные **реальные** из 1С. Если за сегодня уже считали — берётся кэш. Если кэша за месяц нет — запускаются скрипты 1С: `step1_scan` → `fetch_costs` → `step2_calc`. План: 28 450 241 руб/мес.

| Код | Причина |
|-----|---------|
| 400 | Не задано подразделение |
| 403 | Подразделение не в ветке иерархии |
| 404 | Подразделение не найдено в справочнике |

---

### 9. `GET /api/kpi/all/` — KPI всех подчинённых

Без параметров — KPI всех подразделений в ветке. С `?department=` — конкретное подразделение.

**Ответ 200 (без параметра):**

```json
{"departments": [{"department": "...", "kpi_count": 8, "kpis": [...]}, ...]}
```

**Ответ 200 (с `?department=Отдел продаж БМИ`):**

```json
{"department": "Отдел продаж БМИ", "kpi_count": 5, "kpis": [...]}
```

| Код | Причина |
|-----|---------|
| 403 | Нет доступа к подразделению |
| 404 | Подразделение не найдено |

---

### 10. `GET /api/kpi/departments/` — Список подразделений

**Ответ 200:** `{"departments": ["АХО", "ГСПП", "Коммерческий директор", "Коммерческая служба", ...]}`

---

### 11. `GET /api/kpi/structure/` — Иерархия подразделений

**Ответ 200:**

```json
{
  "structure": {
    "коммерческий директор": {
      "Заместитель коммерческого директора по развитию продаж": {
        "Отдел рекламы и PR": {},
        "Отдел ВЭД": {},
        "Отдел продаж БМИ": {},
        "Отдел по работе с ключевыми клиентами": {},
        "Отдел по работе с ПАО «Газпром»": {},
        "Отдел дилерских продаж промышленного оборудования": {}
      },
      "Отдел рекламы и PR": {},
      "Отдел ВЭД": {},
      "Отдел продаж БМИ": {}
    }
  }
}
```

---

## Валовая прибыль — как работает

1. Клиент вызывает `GET /api/kpi/?department=Коммерческий директор`
2. Для KPI `KD-M1` / `KD-Y1` бэкенд проверяет файл `dashboard/vp_result_cache.json`
3. Если кэш **за сегодня** есть — возвращает из него
4. Если нет — для каждого месяца (январь → текущий) проверяет кэш 1С (`вп_<месяц>_<год>_cache.json`)
5. Если кэш 1С за месяц отсутствует — запускает скрипты:
   - `step1_scan.py <месяц> <год>` (~8–10 мин) — скан регистра `ВыручкаИСебестоимостьПродаж`
   - `fetch_costs.py <месяц> <год>` (~20 сек) — загрузка unit-cost из `СтоимостьТоваров`
   - `step2_calc.py <месяц> <год>` (~1 сек) — финальный расчёт
6. Формула: `ВП = Выручка - Стоимость - ДопРасходы - Трудозатраты - Постатейные - РасходыНаПродажу`
7. KPI = `Факт ВП / План (28 450 241) × 100%`
8. Результат кэшируется на день

---

## Роли

| Роль | Приоритет | Описание |
|------|-----------|----------|
| User1 | 1 | Администратор (полный доступ) |
| User2 | 2 | Повышенный доступ |
| User3 | 3 | Средний доступ |
| User4 | 4 | Базовый доступ |
| User5 | 5 | Минимальный доступ (по умолчанию) |

---

## package.json

| Поле | Описание |
|------|----------|
| `secretKey` | Django SECRET_KEY (переопределяется env `SECRET_KEY`) |
| `database.name` | Имя БД PostgreSQL |
| `database.user` | Пользователь PostgreSQL |
| `database.password` | Пароль (переопределяется env `DB_PASSWORD`) |
| `database.host` | Хост БД |
| `database.port` | Порт БД |
