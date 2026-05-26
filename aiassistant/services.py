from __future__ import annotations

import json
import os
import queue
import sys
import threading
from pathlib import Path
from typing import Any, Iterator

from django.conf import settings

_agent_lock = threading.Lock()
_agent_runtime_lock = threading.Lock()
_agent_instance = None


def _ensure_agent_paths() -> None:
    paths = [
        Path(settings.AI_ASSISTANT_AGENT_SRC),
        Path(settings.AI_ASSISTANT_MCP_SRC),
    ]
    for path in paths:
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)
    os.environ.setdefault('EXPLORER_MCP_CONFIG', str(settings.AI_ASSISTANT_MCP_CONFIG))


def _get_agent():
    global _agent_instance
    _ensure_agent_paths()
    with _agent_lock:
        if _agent_instance is None:
            from autonomous_agent.chat_agent import ChatAgent

            _agent_instance = ChatAgent(settings.AI_ASSISTANT_PROJECT_ROOT)
        return _agent_instance


def _event(event_type: str, **payload: Any) -> dict[str, Any]:
    return {'type': event_type, **payload}


def _json_line(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False) + '\n').encode('utf-8')


def _format_user_context(user_context: dict[str, Any] | None) -> str:
    if not user_context:
        return "- Пользователь: не определён\n"
    lines = ["- Пользователь, который задал запрос:"]
    for key, label in (
        ("id", "ID"),
        ("nickname", "Логин"),
        ("role", "Роль"),
        ("department", "Подразделение"),
        ("is_admin", "Администратор"),
    ):
        value = user_context.get(key)
        if value not in (None, ""):
            lines.append(f"  - {label}: {value}")
    return "\n".join(lines) + "\n"


def _augment_message(message: str, user_context: dict[str, Any] | None = None) -> str:
    return (
        f"{message}\n\n"
        "Системный контекст интеграции:\n"
        f"- Корень доступного проекта: {settings.AI_ASSISTANT_PROJECT_ROOT}\n"
        f"{_format_user_context(user_context)}"
        "- Это монорепозиторий dash: DashboardBack, DashboardFrontend, mobile.\n"
        "- Если вопрос про KPI/плитку/дашборд, ищи не только JSON-конфиги. "
        "В этом проекте плитки часто описаны и рассчитываются в "
        "DashboardBack/getkpi/views.py, DashboardBack/getkpi/calc_*.py, "
        "DashboardBack/getkpi/management/commands/*.py, а визуализация в "
        "DashboardFrontend/js/pages/dashboard-*.js.\n"
        "- Для производственных плиток проверяй идентификаторы PD-M*, "
        "PROD_DEPUTY и файлы calc_prod_deputy*.py."
    )


def stream_agent_answer(
    message: str,
    *,
    selected_file: str | None = None,
    user_label: str = '',
    user_context: dict[str, Any] | None = None,
) -> Iterator[bytes]:
    events: queue.Queue[dict[str, Any] | None] = queue.Queue()

    def put(event_type: str, **payload: Any) -> None:
        events.put(_event(event_type, **payload))

    def run() -> None:
        try:
            put(
                'status',
                message='Запускаю AI-ассистента…',
                user=user_label,
                project_root=str(settings.AI_ASSISTANT_PROJECT_ROOT),
            )
            agent = _get_agent()
            with _agent_runtime_lock:
                answer = agent.ask(
                    _augment_message(message, user_context),
                    selected_file=selected_file,
                    on_status=lambda text: put('status', message=text),
                    on_plan=lambda text: put('plan', content=text),
                    on_tool=lambda name: put('tool', name=name),
                    on_file=lambda path: put('file', path=path),
                )
            put('answer', content=answer)
            put('done')
        except Exception as exc:
            put('error', message=str(exc))
        finally:
            events.put(None)

    threading.Thread(target=run, name='ai-assistant-agent', daemon=True).start()
    yield _json_line(_event('ready', message='Соединение с ассистентом установлено.'))

    while True:
        item = events.get()
        if item is None:
            break
        yield _json_line(item)
