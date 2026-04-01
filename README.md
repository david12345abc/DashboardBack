# Dashbord Backend

Бэкенд-часть проекта Dashbord на Django 6 + PostgreSQL.

## Структура проекта

```
Dashbord/
├── manage.py               # Точка входа Django
├── package.json            # Конфигурация подключений (БД, секретный ключ)
├── requirements.txt        # Python-зависимости
├── .gitignore
├── Dashbord/               # Главный модуль Django
│   ├── settings.py         # Настройки (читает package.json)
│   ├── urls.py             # Корневые маршруты
│   ├── wsgi.py
│   └── asgi.py
└── User/                   # Приложение «Пользователи»
    ├── models.py           # Модель User
    ├── views.py            # API-эндпоинты
    ├── urls.py             # Маршруты приложения
    ├── admin.py            # Регистрация в Django Admin
    └── migrations/         # Миграции БД
```

## package.json

Файл хранит конфигурацию, которую `settings.py` считывает при старте.

| Поле                | Описание                                             |
|---------------------|------------------------------------------------------|
| `secretKey`         | Django SECRET_KEY (переопределяется env `SECRET_KEY`) |
| `database.name`     | Имя базы данных PostgreSQL                           |
| `database.user`     | Пользователь PostgreSQL                              |
| `database.password` | Пароль (переопределяется env `DB_PASSWORD`)           |
| `database.host`     | Хост БД                                              |
| `database.port`     | Порт БД                                              |

## Запуск

### 1. Установка зависимостей

```bash
python -m venv venv
venv\Scripts\Activate        # Windows
pip install -r requirements.txt
```

### 2. Настройка БД

Заполните поля в `package.json` (name, user, password, host, port).
База данных должна быть создана заранее:

```sql
CREATE DATABASE dashbord;
```

### 3. Миграции

```bash
python manage.py makemigrations
python manage.py migrate
```

### 4. Запуск сервера

```bash
python manage.py runserver
```

Сервер запустится на `http://127.0.0.1:8000/`.

## User — приложение «Пользователи»

### Модель

- **nickname** — уникальный логин
- **password** — хешированный пароль (Django `make_password` / `check_password`)
- **role** — роль пользователя (см. ниже)
- **created_at** — дата создания

### Роли

| Роль  | Приоритет | Описание          |
|-------|-----------|-------------------|
| User1 | 1         | Администратор     |
| User2 | 2         | Повышенный доступ |
| User3 | 3         | Средний доступ    |
| User4 | 4         | Базовый доступ    |
| User5 | 5         | Минимальный доступ (по умолчанию) |

Чем меньше число приоритета, тем выше привилегии. `User1` — админ.

### API

#### Регистрация пользователя

```
POST /api/user/register/
Content-Type: application/json
```

Тело запроса:

```json
{
  "nickname": "john",
  "password": "secret123",
  "role": "User1"
}
```

`role` не обязателен — по умолчанию `User5`.

Ответ `201`:

```json
{
  "id": 1,
  "nickname": "john",
  "role": "User1",
  "created_at": "2026-04-01T12:00:00+00:00"
}
```

Возможные ошибки:

| Код | Причина                              |
|-----|--------------------------------------|
| 400 | Не передан nickname или password     |
| 400 | Невалидная роль                      |
| 409 | Пользователь с таким nickname уже есть |
