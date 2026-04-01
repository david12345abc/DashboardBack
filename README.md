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
└── User/                   # Приложение «Пользователи»
    ├── models.py
    ├── views.py
    ├── urls.py
    ├── admin.py
    ├── management/commands/createsuperadmin.py
    └── migrations/
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

## API эндпоинты

Базовый путь: `/api/user/`

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
    "department": "IT",
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
  "department": "Отдел продаж"
}
```

| Поле | Тип | Обязательно | Описание |
|------|-----|-------------|----------|
| nickname | string | да | Уникальный логин |
| password | string | да | Пароль |
| role | string | нет | Роль (User1–User5, по умолчанию User5) |
| department | string | да | Название подразделения |

**Ответ 201:**

```json
{
  "id": 2,
  "nickname": "john",
  "role": "User3",
  "department": "Отдел продаж",
  "created_at": "2026-04-01T12:00:00+00:00"
}
```

**Ошибки:**

| Код | Причина |
|-----|---------|
| 400 | Не передан nickname / password |
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
  "department": "IT",
  "created_at": "2026-04-01T12:00:00+00:00"
}
```

**Ошибки:**

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
  "department": "Отдел продаж",
  "created_at": "2026-04-01T12:00:00+00:00"
}
```

**Ошибки:**

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
    {
      "id": 1,
      "nickname": "admin",
      "role": "User1",
      "department": "IT",
      "created_at": "2026-04-01T12:00:00+00:00"
    },
    {
      "id": 2,
      "nickname": "john",
      "role": "User3",
      "department": "Отдел продаж",
      "created_at": "2026-04-01T12:00:00+00:00"
    }
  ]
}
```

**Ошибки:**

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

**Ошибки:**

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

**Body** (все поля необязательные, передавать только то, что нужно изменить):

```json
{
  "nickname": "new_name",
  "password": "new_pass",
  "role": "User2",
  "department": "Новый отдел"
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
  "department": "Новый отдел",
  "created_at": "2026-04-01T12:00:00+00:00"
}
```

**Ошибки:**

| Код | Причина |
|-----|---------|
| 400 | nickname / password пустой |
| 401 | Нет токена / токен невалиден |
| 403 | Нет прав (не свой профиль и не User1) |
| 403 | Смена роли не-админом |
| 404 | Пользователь не найден |
| 409 | nickname уже занят |

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
