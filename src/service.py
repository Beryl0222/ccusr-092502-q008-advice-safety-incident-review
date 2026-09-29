"""应用服务：复盘命令门面、outbox 通知派发与故障恢复。

所有写操作走"聚合锁内重读—决策—追加"，事件日志是唯一事实源；通知任务
日志是可重放的派生物。网关在事件锁之外调用，任务 pending 先持久化，任何
崩溃点之后执行 :meth:`ReviewService.run_recovery` 都得到同一确定结果。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from src.domain import SafetyIncident
from src.notifications import (
    ExposureDirectory,
    FakeGateway,
    GatewayError,
    NotificationGateway,
    TERMINAL_STATUSES,
    idempotency_key,
)
from src.store import EventStore, TaskStore


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ReviewService:
    def __init__(self, root: str, directory: ExposureDirectory,
                 gateway: NotificationGateway | None = None,
                 clock: Callable[[], str] = utc_now_iso) -> None:
        self.store = EventStore(root)
        self.tasks = TaskStore(root)
        self.directory = directory
        self.gateway: NotificationGateway = gateway or FakeGateway()
        self.clock = clock

    def _new_id(self, prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:12]}"

    # ---- 内部：加锁读改写 ----
    def _load(self, incident_id: str) -> SafetyIncident:
        return SafetyIncident.from_events(self.store.load(incident_id))

    def _mutate(self, incident_id: str,
                producer: Callable[[SafetyIncident], dict | list[dict] | None]
                ) -> list[dict]:
        """聚合锁内重读—决策—追加；锁内不会有并发写者。"""
        with self.store.locked_incident(incident_id):
            aggregate = self._load(incident_id)
            result = producer(aggregate)
            if result is None:
                return []
            events = result if isinstance(result, list) else [result]
            for offset, event in enumerate(events):
                self.store.append_unlocked(
                    incident_id, event, aggregate.version + offset
                )
            return events

    def _find_incident_by_version(self, advice_version_id: str) -> str | None:
        for incident_id, events in self.store.load_all().items():
            for event in events:
                payload = event["payload"]
                if payload.get("advice_version_id") == advice_version_id:
                    return incident_id
                if payload.get("duplicate_key") == advice_version_id:
                    return incident_id
        return None

    # ---- 命令：上报与合并 ----
    def report_incident(self, *, incident_id: str, report_id: str, reporter: str,
                        advice_version_id: str, question_context: str,
                        known_history: str, clinical_opinion: str,
                        patient_decision: str, outcome: str,
                        follow_up: str) -> dict:
        """同一建议版本的重复上报合并到既有事件，返回写入的事件。"""
        with self.store.registry():
            target = self._find_incident_by_version(advice_version_id)
            if target is not None:
                incident_id = target

            def producer(aggregate: SafetyIncident) -> dict:
                return aggregate.open_or_merge_report(
                    incident_id=incident_id, event_id=self._new_id("evt"),
                    occurred_at=self.clock(), report_id=report_id,
                    reporter=reporter, advice_version_id=advice_version_id,
                    question_context=question_context,
                    known_history=known_history,
                    clinical_opinion=clinical_opinion,
                    patient_decision=patient_decision, outcome=outcome,
                    follow_up=follow_up,
                )

            events = self._mutate(incident_id, producer)
            return events[-1]

    # ---- 命令：证据 ----
    def freeze_evidence(self, incident_id: str, *, frozen_by: str,
                        evidence_refs: list[str], scope_hash: str) -> dict:
        return self._mutate(
            incident_id,
            lambda agg: agg.freeze_evidence(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                frozen_by=frozen_by, evidence_refs=evidence_refs,
                scope_hash=scope_hash,
            ),
        )[-1]

    def append_evidence(self, incident_id: str, *, append_id: str,
                        submitted_by: str, evidence_refs: list[str],
                        reason: str) -> dict:
        return self._mutate(
            incident_id,
            lambda agg: agg.append_evidence(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                append_id=append_id, submitted_by=submitted_by,
                evidence_refs=evidence_refs, reason=reason,
            ),
        )[-1]

    # ---- 命令：双重复核 ----
    def sign_clinical_opinion(self, incident_id: str, *, opinion_id: str,
                              reviewer_id: str, risk_level: str,
                              rationale: str, signed_record_ref: str) -> dict:
        """临床安全负责人签署医疗风险判定。"""
        return self._mutate(
            incident_id,
            lambda agg: agg.sign_opinion(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                opinion_id=opinion_id, reviewer_id=reviewer_id,
                risk_level=risk_level, rationale=rationale,
                signed_record_ref=signed_record_ref,
            ),
        )[-1]

    def request_record_change(self, incident_id: str, *, opinion_id: str,
                              requested_by: str, reason: str | None = None) -> dict:
        """尝试改写已签署医生记录：被拒绝并留痕（产品侧改写同样被拒绝）。"""
        return self._mutate(
            incident_id,
            lambda agg: agg.reject_record_change(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                opinion_id=opinion_id, requested_by=requested_by, reason=reason,
            ),
        )[-1]

    def decide_impact(self, incident_id: str, *, decision_id: str,
                      owner_id: str, affected_versions: list[str],
                      rationale: str) -> dict:
        """产品负责人判定版本影响范围。"""
        return self._mutate(
            incident_id,
            lambda agg: agg.decide_impact(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                decision_id=decision_id, owner_id=owner_id,
                affected_versions=affected_versions, rationale=rationale,
            ),
        )[-1]

    def raise_objection(self, incident_id: str, *, objection_id: str,
                        raised_by: str, content: str) -> dict:
        return self._mutate(
            incident_id,
            lambda agg: agg.raise_objection(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                objection_id=objection_id, raised_by=raised_by, content=content,
            ),
        )[-1]

    # ---- 命令：最低必要提示 ----
    def publish_minimal_notice(self, incident_id: str, *, notice_id: str,
                               published_by: str, message: str,
                               versions: list[str]) -> dict:
        return self._mutate(
            incident_id,
            lambda agg: agg.publish_minimal_notice(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                notice_id=notice_id, published_by=published_by,
                message=message, versions=versions,
            ),
        )[-1]

    # ---- 命令：最终行动 + outbox 通知 ----
    def decide_final_action(self, incident_id: str, *, action_id: str,
                            action: str, rationale: str,
                            covered_versions: list[str],
                            basis_event_versions: list[int]) -> dict:
        """双重复核完成后决定最终行动，并仅面向实际暴露且授权有效者建通知计划。"""
        with self.store.locked_incident(incident_id):
            aggregate = self._load(incident_id)
            action_event = aggregate.decide_final_action(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                action_id=action_id, action=action, rationale=rationale,
                covered_versions=covered_versions,
                basis_event_versions=basis_event_versions,
            )
            self.store.append_unlocked(incident_id, action_event, aggregate.version)
            aggregate.apply(action_event)

            targets = sorted(
                self.directory.targets_for(action_event["payload"]["covered_versions"])
            )
            plan_id = self._new_id("plan")
            plan_event = aggregate.plan_notices(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                plan_id=plan_id, action_id=action_id, recipients=targets,
            )
            self.store.append_unlocked(incident_id, plan_event, aggregate.version)
            aggregate.apply(plan_event)

            for recipient in targets:
                self.tasks.record({
                    "plan_id": plan_id,
                    "recipient": recipient,
                    "idempotency_key": idempotency_key(plan_id, recipient),
                    "incident_id": incident_id,
                    "status": "pending",
                    "attempts": 0,
                    "updated_at": self.clock(),
                })
            plan_ref = plan_id

        self.dispatch_pending(incident_id)
        return action_event

    def _collect_dispatch_work(self, incident_id: str) -> list[tuple[str, str, str]]:
        """锁内对齐任务日志并收集待发送项；不做任何网关 IO。"""
        work: list[tuple[str, str, str]] = []
        with self.store.locked_incident(incident_id):
            aggregate = self._load(incident_id)
            task_map = self.tasks.load()
            for plan in aggregate.plans:
                for recipient in plan["recipients"]:
                    rkey = (plan["plan_id"], recipient)
                    key = idempotency_key(plan["plan_id"], recipient)
                    if rkey in aggregate.receipts:
                        # 事件流已承认：以回执状态为准对齐任务日志
                        status = aggregate.receipt_status[rkey]
                        stored = task_map.get(rkey)
                        if stored is None or stored["status"] != status:
                            self.tasks.record({
                                "plan_id": plan["plan_id"], "recipient": recipient,
                                "idempotency_key": key, "incident_id": incident_id,
                                "status": status,
                                "attempts": stored["attempts"] if stored else 0,
                                "updated_at": self.clock(),
                            })
                        continue
                    if rkey not in task_map:
                        # 任务日志尾部丢失：由事件流重建 pending
                        self.tasks.record({
                            "plan_id": plan["plan_id"], "recipient": recipient,
                            "idempotency_key": key, "incident_id": incident_id,
                            "status": "pending", "attempts": 0,
                            "updated_at": self.clock(),
                        })
                        task_map[rkey] = {"status": "pending", "attempts": 0}
                    if task_map[rkey]["status"] not in TERMINAL_STATUSES:
                        work.append((plan["plan_id"], recipient, key))
        return work

    def dispatch_pending(self, incident_id: str) -> dict[str, int]:
        """派发某事件全部待办通知；可在故障恢复后重复调用，外发恰好生效一次。"""
        work = self._collect_dispatch_work(incident_id)
        delivered = failed = revoked = 0
        for plan_id, recipient, key in work:
            # 授权在发送时再次判定，发送前撤销立即生效
            if not self.directory.is_authorized(recipient):
                self._record_receipt(incident_id, plan_id, recipient,
                                     "authorization_revoked")
                revoked += 1
                continue
            message = self._notice_message(incident_id, plan_id)
            try:
                self.gateway.send(recipient, message, key)
            except GatewayError:
                self._bump_failed(plan_id, recipient, key, incident_id)
                failed += 1
                continue
            self._record_receipt(incident_id, plan_id, recipient, "delivered")
            delivered += 1
        return {"delivered": delivered, "failed": failed,
                "authorization_revoked": revoked}

    def _notice_message(self, incident_id: str, plan_id: str) -> str:
        aggregate = self._load(incident_id)
        plan = next(p for p in aggregate.plans if p["plan_id"] == plan_id)
        action = next(a for a in aggregate.final_actions
                      if a["action_id"] == plan["action_id"])
        return (
            f"[安全复盘通知] 事件 {incident_id} 已形成最终行动：{action['action']}；"
            f"覆盖建议版本 {', '.join(action['covered_versions'])}。"
            "本通知不构成新的诊断或治疗建议。"
        )

    def _record_receipt(self, incident_id: str, plan_id: str, recipient: str,
                        status: str) -> None:
        """回执先入事件流，再对齐任务日志；事件流是唯一事实源。"""
        def producer(aggregate: SafetyIncident) -> dict | None:
            if (plan_id, recipient) in aggregate.receipts:
                return None  # 并发恢复已写入，幂等跳过
            return aggregate.acknowledge_notice(
                event_id=self._new_id("evt"), occurred_at=self.clock(),
                plan_id=plan_id, recipient=recipient,
                gateway="fake-gateway", status=status,
            )

        self._mutate(incident_id, producer)
        stored = self.tasks.load().get((plan_id, recipient), {})
        self.tasks.record({
            "plan_id": plan_id, "recipient": recipient,
            "idempotency_key": idempotency_key(plan_id, recipient),
            "incident_id": incident_id, "status": status,
            "attempts": stored.get("attempts", 0),
            "updated_at": self.clock(),
        })

    def _bump_failed(self, plan_id: str, recipient: str, key: str,
                     incident_id: str) -> None:
        stored = self.tasks.load().get((plan_id, recipient), {})
        self.tasks.record({
            "plan_id": plan_id, "recipient": recipient,
            "idempotency_key": key, "incident_id": incident_id,
            "status": "failed", "attempts": stored.get("attempts", 0) + 1,
            "updated_at": self.clock(),
        })

    def run_recovery(self) -> dict[str, dict[str, int]]:
        """修复断裂尾行并对全部事件重放待办通知；任意次数调用结果确定。"""
        report: dict[str, dict[str, int]] = {}
        for incident_id in self.store.list_incidents():
            report[incident_id] = self.dispatch_pending(incident_id)
        return report

    # ---- 查询 ----
    def get_review(self, incident_id: str) -> dict[str, Any]:
        """一次复盘的依据、异议、最终行动及其覆盖范围与通知回执。"""
        aggregate = self._load(incident_id)
        brief = aggregate.review_brief()
        task_map = self.tasks.load()
        plan_ids = {p["plan_id"] for p in aggregate.plans}
        brief["notification_tasks"] = [
            task for (plan_id, _recipient), task in sorted(task_map.items())
            if plan_id in plan_ids
        ]
        return brief
