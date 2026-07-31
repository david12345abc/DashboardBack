# Ежедневный native restore SQL-бэкапа 1С (erp_pm)

Контур берёт последний `.bak` с `Z:\`, при необходимости копирует его на `D:\`
и восстанавливает базу `erp_pm` в **локальный** SQL Server (без Docker).

> Docker-контур (`docker-compose.yml`, `restore_latest_bak.py`) устарел и не
> используется. Актуальный скрипт: `restore_native.py`.

## Размещение данных

- `Z:\` — источник `.bak` (`erp_pm_backup_*.bak`);
- `D:\` — локальная копия `.bak` (`erp_pm*.bak`);
- `D:\mssql\data` — файлы данных SQL Server (`.mdf`, `.ndf`);
- `C:\mssql\logdata` — файлы журнала транзакций SQL Server (`.ldf`);
- `D:\mssql\logs` — текстовые журналы скрипта восстановления;
- `D:\mssql\state.json` — отпечаток последнего успешно восстановленного файла.

## Поведение

Каждый день в **21:00** (планировщик Django в `getkpi/restore_scheduler.py`):

1. Берёт последний `*.bak` на `Z:\`.
2. Сравнивает с `D:\erp_pm*.bak` (имя + размер + mtime).
3. Если совпадает и база `ONLINE` — ничего не делает.
4. Если на `Z:\` новее:
   - удаляет старый `D:\erp_pm*.bak`;
   - `DROP DATABASE erp_pm` (+ purge orphan `.mdf/.ndf/.ldf`);
   - копирует дамп `Z:\ → D:\` (~100 ГиБ, resumable);
   - `RESTORE DATABASE … WITH MOVE` (data → `D:\mssql\data`, log → `C:\mssql\logdata`);
   - ждёт `ONLINE`;
   - запускает `run_commercial_cache_refresh_once(force=True)`.

Одновременно второй запуск блокируется (`D:\mssql\restore.lock`).

## Настройка

Скопируйте параметры из `../.env.example` в `../.env`. Ключевые значения:

```dotenv
MSSQL_BAK_SOURCE=Z:\
MSSQL_BAK_DEST=D:\
MSSQL_DATA_DIR=D:/mssql/data
MSSQL_LOG_DATA_DIR=C:/mssql/logdata
MSSQL_LOG_DIR=D:/mssql/logs
MSSQL_RESTORE_DISABLED=0
MSSQL_RESTORE_SCHEDULER_ENABLED=1
MSSQL_RESTORE_HOUR=21
MSSQL_RESTORE_MINUTE=0
SQL_SERVER=.
```

Kill-switch:

- `MSSQL_RESTORE_DISABLED=1` в `.env`, или
- файл-маркер `D:\mssql\RESTORE_DISABLED`.

Нужны: локальный SQL Server, `sqlcmd`, права Windows-учётки процесса на `Z:`,
`D:`, `C:\mssql\logdata` и на DROP/RESTORE.

## Ручной запуск

Из каталога DashboardBack:

```powershell
py -m sql_restore.restore_native
# или
py manage.py restore_erp_pm
# только restore без прогрева комдира:
py manage.py restore_erp_pm --no-commercial
# принудительный полный цикл:
py manage.py restore_erp_pm --force
```

PowerShell-обёртка:

```powershell
.\sql_restore\run_restore.ps1
```

Прогресс и ошибки пишутся в `D:\mssql\logs\restore_YYYYMMDD.log`.

Проверка базы:

```powershell
sqlcmd -S . -E -C -Q "SELECT name, state_desc FROM sys.databases WHERE name = N'erp_pm'"
```

## Планировщик

Стартует вместе с Django (`getkpi.apps.GetkpiConfig.ready`), только в процессе
с `RUN_MAIN=true` (реальный `runserver`, не reloader).

Отключить планировщик, оставив ручной запуск:

```dotenv
MSSQL_RESTORE_SCHEDULER_ENABLED=0
```

Старый Windows Task Scheduler / Docker compose больше не нужны.
