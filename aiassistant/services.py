from __future__ import annotations

import json
import os
import queue
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from django.conf import settings

_agent_lock = threading.Lock()
_agent_runtime_lock = threading.Lock()
_jobs_lock = threading.RLock()
_job_cancel_flags: dict[str, threading.Event] = {}
_agent_instance = None

TERMINAL_STATUSES = {'done', 'error', 'cancelled'}


class JobCancelled(RuntimeError):
    pass


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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jobs_dir() -> Path:
    path = Path(settings.AI_ASSISTANT_PROJECT_ROOT) / '.agentTurbo' / 'assistant_jobs'
    path.mkdir(parents=True, exist_ok=True)
    return path


def _job_path(job_id: str) -> Path:
    return _jobs_dir() / f'{job_id}.json'


def _read_job(job_id: str) -> dict[str, Any] | None:
    with _jobs_lock:
        path = _job_path(job_id)
        if not path.exists():
            return None
        try:
            with path.open('r', encoding='utf-8') as fh:
                return json.load(fh)
        except (OSError, json.JSONDecodeError):
            return None


def _write_job(job: dict[str, Any]) -> None:
    with _jobs_lock:
        path = _job_path(str(job['id']))
        tmp = path.with_name(f'{path.stem}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp')
        try:
            with tmp.open('w', encoding='utf-8') as fh:
                json.dump(job, fh, ensure_ascii=False, indent=2)
            for attempt in range(8):
                try:
                    tmp.replace(path)
                    return
                except PermissionError:
                    if attempt == 7:
                        raise
                    time.sleep(0.05 * (attempt + 1))
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass


def _append_job_event(assistant_job_id: str, event_type: str, **payload: Any) -> dict[str, Any]:
    with _jobs_lock:
        job = _read_job(assistant_job_id)
        if job is None:
            raise RuntimeError(f'AI job not found: {assistant_job_id}')
        events = list(job.get('events') or [])
        event = {
            'seq': len(events),
            'at': _now(),
            'type': event_type,
            **payload,
        }
        events.append(event)
        job['events'] = events
        job['updated_at'] = event['at']
        if event_type in {'answer', 'error'}:
            job['answer'] = payload.get('content') if event_type == 'answer' else ''
        if event_type in TERMINAL_STATUSES:
            job['status'] = event_type
            job['finished_at'] = event['at']
        elif event_type not in {'ready', 'job'} and job.get('status') not in TERMINAL_STATUSES:
            job['status'] = 'running'
        _write_job(job)
        return event


def _user_owns_job(job: dict[str, Any], user_context: dict[str, Any] | None) -> bool:
    job_user = job.get('user') or {}
    current_user = user_context or {}
    for key in ('id', 'nickname', 'department'):
        expected = str(job_user.get(key) or '').strip()
        actual = str(current_user.get(key) or '').strip()
        if expected and actual:
            return expected == actual
    return False


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


def create_assistant_job(
    message: str,
    *,
    selected_file: str | None = None,
    user_label: str = '',
    user_context: dict[str, Any] | None = None,
    conversation_id: str | None = None,
) -> dict[str, Any]:
    job_id = uuid.uuid4().hex
    cancel_flag = threading.Event()
    _job_cancel_flags[job_id] = cancel_flag
    job = {
        'id': job_id,
        'status': 'queued',
        'message': message,
        'selected_file': selected_file,
        'conversation_id': conversation_id or '',
        'user_label': user_label,
        'user': user_context or {},
        'events': [],
        'answer': '',
        'created_at': _now(),
        'updated_at': _now(),
        'finished_at': None,
    }
    with _jobs_lock:
        _write_job(job)
    _append_job_event(job_id, 'job', job_id=job_id, status='queued')
    threading.Thread(
        target=_run_job,
        args=(job_id, message, selected_file, user_label, user_context, conversation_id, cancel_flag),
        name=f'ai-assistant-job-{job_id[:8]}',
        daemon=True,
    ).start()
    return _read_job(job_id) or job


def _run_job(
    job_id: str,
    message: str,
    selected_file: str | None,
    user_label: str,
    user_context: dict[str, Any] | None,
    conversation_id: str | None,
    cancel_flag: threading.Event,
) -> None:
    def check_cancel() -> None:
        if cancel_flag.is_set():
            raise JobCancelled('Запрос остановлен пользователем')

    def put(event_type: str, **payload: Any) -> None:
        check_cancel()
        _append_job_event(job_id, event_type, **payload)

    try:
        put(
            'status',
            message='Запускаю AI-ассистента…',
            user=user_label,
            project_root=str(settings.AI_ASSISTANT_PROJECT_ROOT),
        )
        agent = _get_agent()
        with _agent_runtime_lock:
            check_cancel()
            answer = agent.ask(
                _augment_message(message, user_context),
                selected_file=selected_file,
                conversation_id=conversation_id,
                on_status=lambda text: put('status', message=text),
                on_plan=lambda text: put('plan', content=text),
                on_tool=lambda name: put('tool', name=name),
                on_file=lambda path: put('file', path=path),
            )
            check_cancel()
        _append_job_event(job_id, 'answer', content=answer)
        _append_job_event(job_id, 'done')
    except JobCancelled as exc:
        _append_job_event(job_id, 'cancelled', message=str(exc))
    except Exception as exc:
        _append_job_event(job_id, 'error', message=str(exc))
    finally:
        _job_cancel_flags.pop(job_id, None)


def stop_assistant_job(job_id: str, user_context: dict[str, Any] | None = None) -> dict[str, Any] | None:
    with _jobs_lock:
        job = _read_job(job_id)
        if job is None or not _user_owns_job(job, user_context):
            return None
        if job.get('status') in TERMINAL_STATUSES:
            return job
        job['cancel_requested'] = True
        job['updated_at'] = _now()
        _write_job(job)
    flag = _job_cancel_flags.get(job_id)
    if flag:
        flag.set()
    _append_job_event(job_id, 'status', message='Останавливаю запрос…')
    _append_job_event(job_id, 'cancelled', message='Запрос остановлен пользователем')
    return _read_job(job_id)


def get_assistant_job(job_id: str, user_context: dict[str, Any] | None = None) -> dict[str, Any] | None:
    job = _read_job(job_id)
    if job is None or not _user_owns_job(job, user_context):
        return None
    return job


def stream_job_events(
    job_id: str,
    *,
    after: int = -1,
    user_context: dict[str, Any] | None = None,
    timeout_sec: int = 180,
) -> Iterator[bytes]:
    started = time.monotonic()
    sent = int(after)
    yield _json_line(_event('ready', message='Соединение с ассистентом установлено.'))
    while True:
        job = get_assistant_job(job_id, user_context)
        if job is None:
            yield _json_line(_event('error', message='AI job не найден или недоступен'))
            return
        events = list(job.get('events') or [])
        for event in events:
            seq = int(event.get('seq', -1))
            if seq > sent:
                sent = seq
                yield _json_line(event)
        if job.get('status') in TERMINAL_STATUSES and sent >= len(events) - 1:
            return
        if time.monotonic() - started > timeout_sec:
            yield _json_line(_event('stream_timeout', job_id=job_id, after=sent))
            return
        time.sleep(0.7)
