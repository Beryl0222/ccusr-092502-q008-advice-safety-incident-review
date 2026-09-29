"""持久化：每聚合一个 JSONL 追加日志 + 通知任务日志。

并发：``fcntl`` 咨询锁串行化"读尾版本—追加"，配合 expected-version 形成
乐观并发，冲突方收到确定错误后重试。

故障恢复：打开日志时修复崩溃留下的断裂尾行（最后一条未写完的 JSON 行被
截断），fsync 保证已提交行不丢；通知任务状态可由事件流重新推导，因此任务
日志丢失尾部也能得到一致结果。
"""
from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from typing import Any

from src.domain import EVENT_TYPES
from src.envelope import validate_event


class ConcurrencyConflict(Exception):
    """期望版本与日志尾版本不一致，调用方必须重读后重试。"""


class CorruptLog(Exception):
    """日志中间损坏（非尾行），无法安全继续。"""


def _repair_torn_tail(path: str) -> None:
    """截断最后一条不完整/不可解析的记录；中间损坏直接报错。"""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return
    with open(path, "rb") as handle:
        data = handle.read()
    lines = data.split(b"\n")
    # 末尾换行产生空尾段，属正常
    trailing_empty = lines[-1] == b""
    segments = lines[:-1] if trailing_empty else lines
    bad_from: int | None = None
    for index in range(len(segments) - 1, -1, -1):
        try:
            json.loads(segments[index].decode("utf-8"))
            break
        except (ValueError, UnicodeDecodeError):
            if index != len(segments) - 1:
                raise CorruptLog(f"{path} 中间记录损坏，拒绝自动修复")
            bad_from = index
            break
    if bad_from is None:
        return
    keep = b"\n".join(segments[:bad_from])
    if keep:
        keep += b"\n"
    with open(path, "r+b") as handle:
        handle.seek(0)
        handle.truncate()
        handle.write(keep)
        handle.flush()
        os.fsync(handle.fileno())


def _read_jsonl(path: str, repair: bool = True) -> list[dict]:
    if repair:
        _repair_torn_tail(path)
    if not os.path.exists(path):
        return []
    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _append_jsonl(path: str, record: dict) -> None:
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(line + "\n")
        handle.flush()
        os.fsync(handle.fileno())


class EventStore:
    """事件日志存储；事件一经提交不可变。"""

    def __init__(self, root: str) -> None:
        self.root = root
        self.events_dir = os.path.join(root, "events")
        self.locks_dir = os.path.join(root, "locks")
        os.makedirs(self.events_dir, exist_ok=True)
        os.makedirs(self.locks_dir, exist_ok=True)
        self._registry_lock = os.path.join(self.locks_dir, "registry.lock")

    def _path(self, incident_id: str) -> str:
        return os.path.join(self.events_dir, f"{incident_id}.jsonl")

    @contextmanager
    def _locked(self, name: str):
        lock_path = os.path.join(self.locks_dir, f"{name}.lock")
        with open(lock_path, "a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def locked_incident(self, incident_id: str):
        """聚合串行锁；append_unlocked 必须在本锁内调用，禁止嵌套同名锁。"""
        with self._locked(f"event-{incident_id}"):
            yield

    def exists(self, incident_id: str) -> bool:
        path = self._path(incident_id)
        return os.path.exists(path) and os.path.getsize(path) > 0

    def list_incidents(self) -> list[str]:
        return sorted(
            name[: -len(".jsonl")]
            for name in os.listdir(self.events_dir)
            if name.endswith(".jsonl")
        )

    def load(self, incident_id: str) -> list[dict]:
        events = _read_jsonl(self._path(incident_id))
        seen_versions: set[int] = set()
        for event in events:
            errors = validate_event(event, set(EVENT_TYPES))
            if errors:
                raise CorruptLog(f"{incident_id} 存在非法事件: {errors}")
            if event["aggregate_id"] != incident_id:
                raise CorruptLog(f"{incident_id} 混入其他聚合事件")
            version = event["version"]
            if version in seen_versions or version != len(seen_versions) + 1:
                raise CorruptLog(f"{incident_id} 事件版本不连续")
            seen_versions.add(version)
        return events

    def load_all(self) -> dict[str, list[dict]]:
        return {incident_id: self.load(incident_id)
                for incident_id in self.list_incidents()}

    def append(self, incident_id: str, event: dict, expected_version: int) -> None:
        """在聚合锁内重读尾版本并追加；冲突抛 ConcurrencyConflict。"""
        with self._locked(f"event-{incident_id}"):
            self.append_unlocked(incident_id, event, expected_version)

    def append_unlocked(self, incident_id: str, event: dict,
                        expected_version: int) -> None:
        """同 :meth:`append`，但假定调用方已持有 :meth:`locked_incident` 锁。"""
        errors = validate_event(event, set(EVENT_TYPES))
        if errors:
            raise ValueError(f"非法事件: {errors}")
        if event["aggregate_id"] != incident_id:
            raise ValueError("事件 aggregate_id 与目标流不一致")
        tail_version = len(_read_jsonl(self._path(incident_id)))
        if expected_version != tail_version:
            raise ConcurrencyConflict(
                f"期望版本 {expected_version}，日志尾版本 {tail_version}"
            )
        if event["version"] != tail_version + 1:
            raise ValueError(
                f"事件版本 {event['version']} 不等于尾版本 {tail_version} + 1"
            )
        _append_jsonl(self._path(incident_id), event)

    @contextmanager
    def registry(self):
        """全局注册锁：保护"按建议版本查重—创建新事件流"。"""
        with self._locked("registry"):
            yield


class TaskStore:
    """通知任务的追加日志；同一 (plan_id, recipient) 的最新记录为准。"""

    def __init__(self, root: str) -> None:
        self.root = root
        os.makedirs(root, exist_ok=True)
        self.path = os.path.join(root, "tasks.jsonl")
        self.locks_dir = os.path.join(root, "locks")
        os.makedirs(self.locks_dir, exist_ok=True)
        self._lock_path = os.path.join(self.locks_dir, "tasks.lock")

    @contextmanager
    def _locked(self):
        with open(self._lock_path, "a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def load(self) -> dict[tuple[str, str], dict]:
        records = _read_jsonl(self.path)
        tasks: dict[tuple[str, str], dict] = {}
        for record in records:
            tasks[(record["plan_id"], record["recipient"])] = record
        return tasks

    def record(self, task: dict[str, Any]) -> None:
        with self._locked():
            _append_jsonl(self.path, task)
