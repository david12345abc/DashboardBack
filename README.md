# Dashbord Backend

Бэкенд на Django 6 + PostgreSQL.

---

## Структура проекта

```
Dashbord/
├── manage.py
├── package.json            # Конфигурация (БД, секретный ключ)
├── requirements.txt
├── .gitignore
├── Dashbord/               # Главный модуль Django
│   ├── settings.py
│   ├── urls.py
│   ├── wsgi.py
│   └── asgi.py
├── User/                   # Приложение «Пользователи»
│   ├── models.py
│   ├── views.py
│   ├── urls.py
│   ├── admin.py
│   ├── management/commands/createsuperadmin.py
│   └── migrations/
└── getkpi/                 # Приложение «KPI подразделений»
    ├── views.py
    ├── urls.py
    ├── kpi_data.json       # Справочник KPI (26 подразделений, 151 KPI)
    └── temp/               # Исходные файлы Excel / Word
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

Все защищённые эндпоинты требуют **JWT-токен** в заголовке:

```
Authorization: Bearer <token>
```

Токен выдаётся при логине. Срок жизни — **7 дней**.

---

## API — User (`/api/user/`)

---

### 1. Логин

|||
|---|---|
| **URL** | `POST /api/user/login/` |
| **Доступ** | Все (без токена) |

**Headers:**

```
Content-Type: application/json
```

**Body:**

```json
{
  "nickname": "admin",
  "password": "secret123"
}
```

| Поле | Тип | Обязательно | Описание |
|------|-----|-------------|----------|
| nickname | string | да | Логин пользователя |
| password | string | да | Пароль |

**Ответ 200:**

```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": 1,
    "nickname": "admin",
    "role": "User1",
    "department": "Коммерческая служба",
    "created_at": "2026-04-01T12:00:00+00:00"
  }
}
```

**Ошибки:**

| Код | Причина |
|-----|---------|
| 400 | Не передан nickname или password |
| 401 | Неверный логин или пароль |

---

### 2. Регистрация пользователя

|||
|---|---|
| **URL** | `POST /api/user/register/` |
| **Доступ** | Только **User1** (админ) |

**Headers:**

```
Content-Type: application/json
Authorization: Bearer <token>
```

**Body:**

```json
{
  "nickname": "john",
  "password": "pass123",
  "role": "User3",
  "department": "Коммерческая служба"
}
```

| Поле | Тип | Обязательно | Описание |
|------|-----|-------------|----------|
| nickname | string | да | Уникальный логин |
| password | string | да | Пароль |
| role | string | нет | Роль (User1–User5, по умолчанию User5) |
| department | string | да | Название подразделения (должно совпадать со справочником) |

**Ответ 201:**

```json
{
  "id": 2,
  "nickname": "john",
  "role": "User3",
  "department": "Коммерческая служба",
  "created_at": "2026-04-01T12:00:00+00:00"
}
```

**Ошибки:**

| Код | Причина |
|-----|---------|
| 400 | Не передан nickname / password / department |
| 400 | Невалидная роль |
| 401 | Нет токена / токен невалиден |
| 403 | Текущий пользователь не User1 |
| 409 | nickname уже занят |

---

### 3. Информация о себе

|||
|---|---|
| **URL** | `GET /api/user/me/` |
| **Доступ** | Любой авторизованный |

**Headers:**

```
Authorization: Bearer <token>
```

**Body:** нет

**Ответ 200:**

```json
{
  "id": 1,
  "nickname": "admin",
  "role": "User1",
  "department": "Коммерческая служба",
  "created_at": "2026-04-01T12:00:00+00:00"
}
```

| Код | Причина |
|-----|---------|
| 401 | Нет токена / токен невалиден |

---

### 4. Получить пользователя по ID

|||
|---|---|
| **URL** | `GET /api/user/<id>/` |
| **Доступ** | Только **User1** |

**Headers:**

```
Authorization: Bearer <token>
```

**Body:** нет

**Ответ 200:**

```json
{
  "id": 2,
  "nickname": "john",
  "role": "User3",
  "department": "Коммерческая служба",
  "created_at": "2026-04-01T12:00:00+00:00"
}
```

| Код | Причина |
|-----|---------|
| 401 | Нет токена / токен невалиден |
| 403 | Текущий пользователь не User1 |
| 404 | Пользователь не найден |

---

### 5. Список всех пользователей

|||
|---|---|
| **URL** | `GET /api/user/list/` |
| **Доступ** | Только **User1** |

**Headers:**

```
Authorization: Bearer <token>
```

**Body:** нет

**Ответ 200:**

```json
{
  "users": [
    { "id": 1, "nickname": "admin", "role": "User1", "department": "...", "created_at": "..." },
    { "id": 2, "nickname": "john", "role": "User3", "department": "...", "created_at": "..." }
  ]
}
```

| Код | Причина |
|-----|---------|
| 401 | Нет токена / токен невалиден |
| 403 | Текущий пользователь не User1 |

---

### 6. Удаление пользователя

|||
|---|---|
| **URL** | `POST /api/user/<id>/delete/` |
| **Доступ** | Только **User1** |

**Headers:**

```
Authorization: Bearer <token>
```

**Body:** нет

**Ответ 200:**

```json
{ "message": "User deleted" }
```

| Код | Причина |
|-----|---------|
| 400 | Нельзя удалить самого себя |
| 401 | Нет токена / токен невалиден |
| 403 | Текущий пользователь не User1 |
| 404 | Пользователь не найден |

---

### 7. Редактирование пользователя

|||
|---|---|
| **URL** | `PUT /api/user/<id>/edit/` или `PATCH /api/user/<id>/edit/` |
| **Доступ** | Сам себя — любой авторизованный; чужого — только **User1** |

**Headers:**

```
Content-Type: application/json
Authorization: Bearer <token>
```

**Body** (все поля необязательные):

```json
{
  "nickname": "new_name",
  "password": "new_pass",
  "role": "User2",
  "department": "Сервисная служба"
}
```

| Поле | Тип | Описание |
|------|-----|----------|
| nickname | string | Новый логин |
| password | string | Новый пароль |
| role | string | Новая роль (менять может **только User1**) |
| department | string | Новое подразделение |

**Ответ 200:**

```json
{
  "id": 2,
  "nickname": "new_name",
  "role": "User2",
  "department": "Сервисная служба",
  "created_at": "2026-04-01T12:00:00+00:00"
}
```

| Код | Причина |
|-----|---------|
| 400 | nickname / password пустой |
| 401 | Нет токена / токен невалиден |
| 403 | Нет прав (не свой профиль и не User1) |
| 403 | Смена роли не-админом |
| 404 | Пользователь не найден |
| 409 | nickname уже занят |

---

## API — KPI (`/api/kpi/`)

---

### 8. KPI текущего пользователя (по его подразделению)

|||
|---|---|
| **URL** | `GET /api/kpi/` |
| **Доступ** | Любой авторизованный |

**Headers:**

```
Authorization: Bearer <token>
```

**Body:** нет

Определяет подразделение из `department` пользователя и возвращает все KPI этого подразделения со сгенерированными значениями.

**Ответ 200:**

```json
{
  "department": "Коммерческая служба",
  "kpi_count": 8,
  "kpis": [
    {
      "kpi_id": "КОМ-M1",
      "name": "Выполнение маркетингового плана по деньгам",
      "frequency": "Ежемесячно",
      "period_count": 12,
      "values": [102.3, 95.1, 108.7, 99.4, 101.2, 88.5, 110.3, 97.6, 103.8, 92.1, 105.4, 100.0],
      "success_pct": 58.3,
      "perspective": "Финансы",
      "goal": "Обеспечить выполнение плана поступления ДС коммерческого блока",
      "formula": "Факт поступления ДС / План × 100%",
      "unit": "%",
      "source": "1С",
      "monthly_target": "≥100%",
      "quarterly_target": null,
      "yearly_target": "≥100%",
      "green_threshold": "≥100%",
      "yellow_threshold": "90–99,9%",
      "red_threshold": "<90%",
      "weight_pct": 35
    },
    {
      "kpi_id": "КОМ-Q5",
      "name": "Текучесть персонала коммерческой службы",
      "frequency": "Ежеквартально",
      "period_count": 4,
      "values": [3.2, 4.8, 6.1, 2.9],
      "success_pct": 75.0,
      "perspective": "Персонал",
      "goal": "Сохранить устойчивость коммерческого блока",
      "formula": "Уволенные / Среднесписочная численность × 100%",
      "unit": "%",
      "source": "HR",
      "monthly_target": null,
      "quarterly_target": "≤5%",
      "yearly_target": "≤20%",
      "green_threshold": "≤5%",
      "yellow_threshold": "5,1–7%",
      "red_threshold": ">7%",
      "weight_pct": 30
    }
  ]
}
```

| Код | Причина |
|-----|---------|
| 400 | У пользователя не задано подразделение |
| 401 | Нет токена / токен невалиден |
| 404 | Подразделение не найдено в справочнике KPI (вернёт список доступных) |

---

### 9. KPI всех подразделений

|||
|---|---|
| **URL** | `GET /api/kpi/all/` |
| **Доступ** | Любой авторизованный |

**Headers:**

```
Authorization: Bearer <token>
```

**Body:** нет

**Ответ 200:**

```json
{
  "departments": [
    {
      "department": "Коммерческая служба",
      "kpi_count": 8,
      "kpis": [ ... ]
    },
    {
      "department": "ОВЭД",
      "kpi_count": 7,
      "kpis": [ ... ]
    }
  ]
}
```

| Код | Причина |
|-----|---------|
| 401 | Нет токена / токен невалиден |

---

### 10. Список подразделений

|||
|---|---|
| **URL** | `GET /api/kpi/departments/` |
| **Доступ** | Любой авторизованный |

**Headers:**

```
Authorization: Bearer <token>
```

**Body:** нет

**Ответ 200:**

```json
{
  "departments": [
    "АХО",
    "ГСПП",
    "ИТ / Администрирование / Администрирование 1С",
    "Конструкторское бюро",
    "Коммерческая служба",
    "ОВЭД",
    "ОМТС / Служба снабжения",
    "ОПЭОиУ",
    "Отдел дилерских продаж",
    "Отдел по работе с ПАО «Газпром»",
    "Отдел по работе с ключевыми клиентами",
    "Отдел продаж БМИ",
    "Производственная служба",
    "Производство ЭКО",
    "Производство №1",
    "Производство №2",
    "Проектный офис",
    "Сервисная служба",
    "Склад",
    "Служба безопасности",
    "Служба качества",
    "Служба персонала",
    "Служба стандартов и регламентов",
    "Транспортная служба / диспетчерская",
    "Финансовая служба",
    "Юридическая служба"
  ]
}
```

| Код | Причина |
|-----|---------|
| 401 | Нет токена / токен невалиден |

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
| `database.name` | Имя базы данных PostgreSQL |
| `database.user` | Пользователь PostgreSQL |
| `database.password` | Пароль (переопределяется env `DB_PASSWORD`) |
| `database.host` | Хост БД |
| `database.port` | Порт БД |
