"""
dump_db.py — снятие дампа PostgreSQL-базы проекта.

Креды читаются из package.json (те же, что в settings.py), переопределяются
переменной окружения DB_PASSWORD, если она задана.

Результат: Back/Dashbord/backups/dashbord_YYYY-MM-DD_HH-MM.sql

Использование:
    python dump_db.py                       # полный дамп (schema + data)
    python dump_db.py --schema-only         # только схема
    python dump_db.py --data-only           # только данные
    python dump_db.py --output my_dump.sql  # своё имя файла
    python dump_db.py --pg-dump "C:/Program Files/PostgreSQL/16/bin/pg_dump.exe"

Требования:
    - В PATH должна быть утилита pg_dump (либо передай путь через --pg-dump).
    - При запуске с Windows-клиента PG-клиент должен уметь ходить на host:port,
      указанные в package.json. Если база на сервере и вне LAN — поднимай
      ssh-туннель (ssh -L 5432:localhost:5432 user@host) и указывай host=localhost.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PACKAGE_JSON = BASE_DIR / "package.json"
BACKUPS_DIR = BASE_DIR / "backups"


def load_db_config() -> dict:
    with open(PACKAGE_JSON, "r", encoding="utf-8") as f:
        pkg = json.load(f)
    db = pkg.get("database") or {}
    return {
        "name": db.get("name", "dashbord"),
        "user": db.get("user", "postgres"),
        "password": os.environ.get("DB_PASSWORD") or db.get("password", ""),
        "host": db.get("host", "localhost"),
        "port": str(db.get("port", 5432)),
    }


def resolve_pg_dump(explicit: str | None) -> str:
    if explicit:
        return explicit
    found = shutil.which("pg_dump")
    if found:
        return found
    # Типичные пути установки на Windows
    for ver in ("18", "17", "16", "15", "14", "13"):
        candidate = Path(rf"C:\Program Files\PostgreSQL\{ver}\bin\pg_dump.exe")
        if candidate.exists():
            return str(candidate)
    sys.stderr.write(
        "ERROR: pg_dump не найден в PATH. "
        "Передай путь через --pg-dump или добавь в PATH.\n"
    )
    sys.exit(2)


def build_output_path(custom: str | None) -> Path:
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    if custom:
        p = Path(custom)
        if not p.is_absolute():
            p = BACKUPS_DIR / p
        return p
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    return BACKUPS_DIR / f"dashbord_{stamp}.sql"


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    print("[INFO] dump_db.py started")

    parser = argparse.ArgumentParser(description="Сделать SQL-дамп БД проекта")
    parser.add_argument("--schema-only", action="store_true",
                        help="Только структура, без данных")
    parser.add_argument("--data-only", action="store_true",
                        help="Только данные, без структуры")
    parser.add_argument("--output", "-o", default=None,
                        help="Имя/путь выходного файла (.sql)")
    parser.add_argument("--pg-dump", default=None,
                        help="Путь к pg_dump, если его нет в PATH")
    args = parser.parse_args()

    if args.schema_only and args.data_only:
        parser.error("--schema-only и --data-only нельзя указывать вместе")

    cfg = load_db_config()
    pg_dump = resolve_pg_dump(args.pg_dump)
    out_path = build_output_path(args.output)

    cmd = [
        pg_dump,
        "-h", cfg["host"],
        "-p", cfg["port"],
        "-U", cfg["user"],
        "-d", cfg["name"],
        # plain SQL, всегда создавать владельцев/схемы как есть
        "--format=plain",
        "--encoding=UTF8",
        "--no-owner",
        "--no-privileges",
        "--file", str(out_path),
    ]
    if args.schema_only:
        cmd.append("--schema-only")
    if args.data_only:
        cmd.append("--data-only")

    env = os.environ.copy()
    if cfg["password"]:
        env["PGPASSWORD"] = cfg["password"]

    print(f"[+] pg_dump:  {pg_dump}")
    print(f"[+] host:     {cfg['host']}:{cfg['port']}")
    print(f"[+] db/user:  {cfg['name']} / {cfg['user']}")
    print(f"[+] output:   {out_path}")
    print("[+] запускаю pg_dump...")

    try:
        result = subprocess.run(cmd, env=env, check=False)
    except FileNotFoundError as exc:
        sys.stderr.write(f"ERROR: не удалось запустить pg_dump: {exc}\n")
        sys.exit(2)

    if result.returncode != 0:
        sys.stderr.write(
            f"ERROR: pg_dump завершился с кодом {result.returncode}\n"
        )
        if out_path.exists() and out_path.stat().st_size == 0:
            try:
                out_path.unlink()
            except OSError:
                pass
        sys.exit(result.returncode)

    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Готово. Размер дампа: {size_mb:.1f} МБ")
    print(f"     Файл: {out_path}")


if __name__ == "__main__":
    main()
