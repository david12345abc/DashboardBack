# Ежедневный разворот SQL-бэкапа 1С

Контур берёт последний `.bak` из `\\srv2\copy1cbase` (SMB, смонтирован в
контейнер) и восстанавливает базу `erp_pm` в SQL Server 2022 в Docker.

## Размещение данных

- `D:\mssql\data` — файлы данных SQL Server (`.mdf`, `.ndf`);
- `C:\mssql\log` — файлы журнала транзакций SQL Server (`.ldf`), **постоянно**;
- `D:\mssql\logs` — текстовые журналы скрипта восстановления (не `.ldf`);
- `D:\mssql\incoming` — опциональная локальная копия `.bak` (сейчас не нужна:
  restore идёт напрямую с SMB-тома);
- `D:\mssql\state.json` — отпечаток последнего успешно восстановленного файла.

Источник `.bak`: `SMB_SHARE` (по умолчанию `\\srv2\copy1cbase`), в контейнере
как `/var/opt/mssql/backup` (внешний Docker volume `dashboard-erp-smb-backup`).

Один и тот же файл повторно не восстанавливается. Сравниваются имя, размер и
точное время изменения файла. Состояние обновляется только после успешного
`RESTORE DATABASE` и проверки состояния `ONLINE`.

После restore журналы транзакций **не** переносятся на `D:` — они остаются на
`C:\mssql\log`.

## Настройка

Скопируйте параметры из `../.env.example` в `../.env`. Обязательные значения:

```dotenv
LOGIN=DOMAIN\username
PASSWORD=...
SMB_SHARE=\\srv2\copy1cbase
MSSQL_SA_PASSWORD=...
MSSQL_DATA_DIR=D:/mssql/data
MSSQL_LOG_DATA_DIR=C:/mssql/log
MSSQL_LOG_DIR=D:/mssql/logs
```

Различие путей:

- `MSSQL_LOG_DATA_DIR` — каталог SQL `.ldf` на `C:` (bind → `/var/opt/mssql/log`);
- `MSSQL_LOG_DIR` — текстовые логи Python-скрипта на `D:`.

`.env` исключён из Git. Пароль SQL Server должен содержать буквы в разных
регистрах, цифры и спецсимволы.

Docker Desktop должен быть запущен. В Docker Desktop должен быть разрешён
доступ к дискам `C:\` и `D:\` (в актуальных версиях WSL2 обычно доступны
автоматически).

Compose-проект: `dashboard-erp-sql`, контейнер: `dashboard-erp-mssql`.

## Ручной запуск

Из PowerShell:

```powershell
cd C:\Users\testii\Downloads\dash\DashboardBack
.\sql_restore\run_restore.ps1
```

Первый запуск скачивает образ SQL Server и выполняет полное восстановление.
Это может занять много часов. Прогресс SQL `STATS=5` и опрос до `ONLINE`
записываются в `D:\mssql\logs\restore_YYYYMMDD.log`.

Проверка контейнера:

```powershell
docker ps --filter name=dashboard-erp-mssql
docker logs dashboard-erp-mssql
```

Проверка базы:

```powershell
$env:SQLCMDPASSWORD = (Get-Content .env |
  Where-Object { $_ -like 'MSSQL_SA_PASSWORD=*' }).Split('=', 2)[1]
docker exec -e SQLCMDPASSWORD dashboard-erp-mssql `
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C `
  -Q "SELECT name, state_desc FROM sys.databases WHERE name = N'erp_pm'"
Remove-Item Env:\SQLCMDPASSWORD
```

Если restore уже завершился в `ONLINE`, но `state.json` ещё не обновлён:

```powershell
py .\sql_restore\finish_restore.py
```

Опрос до `ONLINE` без повторного `RESTORE`:

```powershell
py .\sql_restore\poll_restore.py
```

## Ежедневная задача

Регистрация запуска каждый день в 07:00:

```powershell
.\sql_restore\register_task.ps1
```

Другое время:

```powershell
.\sql_restore\register_task.ps1 -DailyAt "08:30"
```

Задача запускается под учётной записью `LOGIN` из `.env`, чтобы одновременно
иметь доступ к сетевой папке, Docker Desktop и дискам `C:\` / `D:\`. Если пароль
доменной учётной записи изменится, запустите `register_task.ps1` повторно.

Проверка задачи:

```powershell
Get-ScheduledTask -TaskName "Dashboard - Daily 1C SQL Restore"
Get-ScheduledTaskInfo -TaskName "Dashboard - Daily 1C SQL Restore"
```

## Поведение при ошибках

- файл моложе `BACKUP_MIN_AGE_MINUTES` не берётся — он может ещё загружаться;
- перед restore проверяется свободное место: `D:` под data, `C:` под `.ldf`
  (~174 GiB+ для этой базы);
- при обрыве клиента restore не перезапускается: скрипт опрашивает до `ONLINE`
  и не выдаёт повторный `WITH REPLACE`, пока база в `RESTORING`;
- при ошибке восстановления `state.json` не обновляется;
- одновременный второй запуск завершается, не вмешиваясь в текущий.
