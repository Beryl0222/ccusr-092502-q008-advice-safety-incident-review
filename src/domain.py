"""应用服务：命令门面。"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from src.domain import SafetyIncident, DomainError


class ReviewError(Exception):
    """复盘工作流的基类。"""


# ---------------------------------------------------------------------------
# 纯函数领域模型（事件溯源）
# ---------------------------------------------------------------------------

class SafetyIncident:
    """复盘聚合根。"""

    def __init__(self, incident_id: str):
        self.incident_id = incident_id
        self.version: int = 0  # 先占位，初始版本号
        self.version = 0

    @classmethod
    def create(cls, **data) -> SafetyIncident:
        raise NotImplementedError

    @classmethod
    def create(cls, **data) -> SafetyIncident:
        raise NotImplementedError
