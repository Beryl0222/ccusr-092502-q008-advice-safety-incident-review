"""领域事件信封的基础校验。"""
from __future__ import annotations

from datetime import datetime
from typing import Any


REQUIRED_FIELDS = (
    "event_id", "event_type", "occurred_at", "aggregate_id", "version", "payload"
)


def validate_event(value: Any, allowed_events: set[str]) -> list[str]:
    """返回稳定的字段错误列表，不执行业务状态转换。"""
    if not isinstance(value, dict):
        return ["事件必须是 JSON 对象"]
    errors: list[str] = []
    for field in REQUIRED_FIELDS:
        if field not in value:
            errors.append(f"缺少字段: {field}")
    if errors:
        return errors
    if not isinstance(value["event_id"], str) or not value["event_id"].strip():
        errors.append("event_id 必须是非空字符串")
    if value["event_type"] not in allowed_events:
        errors.append("未知的 event_type")
    if not isinstance(value["aggregate_id"], str) or not value["aggregate_id"].strip():
        errors.append("aggregate_id 必须是非空字符串")
    if not isinstance(value["version"], int) or isinstance(value["version"], bool) or value["version"] < 1:
        errors.append("version 必须是正整数")
    if not isinstance(value["payload"], dict):
        errors.append("payload 必须是 JSON 对象")
    try:
        parsed = datetime.fromisoformat(str(value["occurred_at"]))
        if parsed.tzinfo is None:
            errors.append("occurred_at 必须包含时区")
    except ValueError:
        errors.append("occurred_at 不是有效时间")
    return errors
