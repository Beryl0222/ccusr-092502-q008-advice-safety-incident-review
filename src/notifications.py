"""通知定位与外发。

定位规则（交集在发送时计算）：
    实际暴露于受影响版本 且 授权在发送时仍有效 的对象。

外发使用 outbox：NOTICE_PLANNED 产生持久 pending 任务，网关按
``plan_id:recipient`` 幂等键投递，结果以 NOTICE_ACKNOWLEDGED 回到事件流。
任务日志只反映事件流已承认的事实，崩溃后可安全重放。
"""
from __future__ import annotations

import fcntl
import json
import os
from datetime import datetime, timezone
from typing import Protocol

from src.store import _append_jsonl, _read_jsonl  # 复用 JSONL 追加与尾行修复


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExposureDirectory:
    """谁在何时接收过哪个建议版本；授权可被撤销，撤销在发送时生效。"""

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._lock_path = path + ".lock"
        self._exposed: dict[str, set[str]] = {}   # recipient -> versions
        self._revoked: set[str] = set()
        for record in _read_jsonl(path):
            self._fold(record)

    def _fold(self, record: dict) -> None:
        if record["kind"] == "EXPOSED":
            self._exposed.setdefault(record["recipient"], set()).add(
                record["advice_version_id"]
            )
        elif record["kind"] == "AUTHORIZATION_REVOKED":
            self._revoked.add(record["recipient"])

    def _write(self, record: dict) -> None:
        with open(self._lock_path, "a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                _append_jsonl(self.path, record)
                self._fold(record)
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def register_exposure(self, recipient: str, advice_version_id: str,
                          exposed_at: str | None = None) -> None:
        self._write({
            "kind": "EXPOSED",
            "recipient": recipient,
            "advice_version_id": advice_version_id,
            "exposed_at": exposed_at or utc_now_iso(),
        })

    def revoke_authorization(self, recipient: str,
                             revoked_at: str | None = None) -> None:
        self._write({
            "kind": "AUTHORIZATION_REVOKED",
            "recipient": recipient,
            "revoked_at": revoked_at or utc_now_iso(),
        })

    def exposed_to_any(self, versions: list[str] | set[str]) -> set[str]:
        wanted = set(versions)
        return {
            recipient
            for recipient, got in self._exposed.items()
            if got & wanted
        }

    def is_authorized(self, recipient: str) -> bool:
        return recipient not in self._revoked

    def targets_for(self, versions: list[str] | set[str]) -> set[str]:
        """实际暴露于受影响版本，且授权在查询时刻仍有效。"""
        return {
            recipient
            for recipient in self.exposed_to_any(versions)
            if self.is_authorized(recipient)
        }


class GatewayError(Exception):
    """网关暂时性故障：任务保持 pending/failed，稍后必须可重试。"""


class NotificationGateway(Protocol):
    def send(self, recipient: str, message: str, idempotency_key: str) -> str:
        """成功返回网关消息 ID；重复幂等键必须返回同一结果而不是二次投递。"""


class FakeGateway:
    """内存假网关：记录幂等键，可注入故障与授权拦截。"""

    def __init__(self) -> None:
        self.sent: dict[str, dict] = {}
        self.fail_keys: set[str] = set()
        self.deliveries: list[dict] = []

    def fail_next(self, idempotency_key: str) -> None:
        self.fail_keys.add(idempotency_key)

    def send(self, recipient: str, message: str, idempotency_key: str) -> str:
        if idempotency_key in self.fail_keys:
            self.fail_keys.discard(idempotency_key)
            raise GatewayError(f"网关暂时不可用: {idempotency_key}")
        if idempotency_key in self.sent:
            # 真实网关按幂等键去重：绝不二次投递
            return self.sent[idempotency_key]["gateway_message_id"]
        message_id = f"gw-{idempotency_key}"
        self.sent[idempotency_key] = {
            "gateway_message_id": message_id,
            "recipient": recipient,
            "message": message,
        }
        self.deliveries.append(self.sent[idempotency_key])
        return message_id


def idempotency_key(plan_id: str, recipient: str) -> str:
    return f"{plan_id}:{recipient}"


TERMINAL_STATUSES = ("delivered", "authorization_revoked")
