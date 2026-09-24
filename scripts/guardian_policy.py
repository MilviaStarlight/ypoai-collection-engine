"""Deterministic safety policy for the NEXA collector guardian.

The policy accepts observations and returns an allow-listed action. It performs
no network, database, workflow, or model calls, which keeps repair authority
separate from advisory AI analysis.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


ALLOWED_ACTIONS = frozenset(
    {
        "NO_ACTION",
        "WAKE_COLLECTOR",
        "RELEASE_STALE_LEASE",
        "STOP_AND_REQUEUE",
        "REQUEUE_FREE_MODEL",
        "QUARANTINE",
    }
)


@dataclass(frozen=True)
class GuardianObservation:
    ready_count: int
    last_collector_heartbeat: datetime | None
    last_progress_at: datetime | None
    repeated_target_count: int
    repeated_error_count: int
    lease_expired: bool
    repair_attempts: int
    error_fingerprint: str | None


@dataclass(frozen=True)
class GuardianDecision:
    action: str
    incident_type: str
    quarantine: bool
    reason: str

    def __post_init__(self) -> None:
        if self.action not in ALLOWED_ACTIONS:
            raise ValueError(f"Guardian action is not allow-listed: {self.action}")


def decide(observation: GuardianObservation, now: datetime) -> GuardianDecision:
    """Return exactly one bounded action for a current runtime observation."""

    if observation.ready_count <= 0:
        return GuardianDecision("NO_ACTION", "NONE", False, "No eligible work")

    if observation.repair_attempts >= 2:
        return GuardianDecision(
            "QUARANTINE", "REPAIR_LIMIT", True, "Two repairs exhausted"
        )

    if (
        observation.repeated_target_count >= 3
        or observation.repeated_error_count >= 3
    ):
        return GuardianDecision(
            "STOP_AND_REQUEUE", "LOOP", False, "Repeated target or error"
        )

    if observation.lease_expired and observation.last_progress_at is not None:
        seconds_without_progress = (
            now - observation.last_progress_at
        ).total_seconds()
        if seconds_without_progress >= 15 * 60:
            return GuardianDecision(
                "RELEASE_STALE_LEASE",
                "STALE_LEASE",
                False,
                "Lease expired without progress",
            )

    if observation.error_fingerprint == "model-rate-limit":
        return GuardianDecision(
            "REQUEUE_FREE_MODEL",
            "FREE_MODEL_UNAVAILABLE",
            False,
            "No paid fallback",
        )

    if observation.last_collector_heartbeat is None:
        return GuardianDecision(
            "WAKE_COLLECTOR", "SLEEPING", False, "Ready work without heartbeat"
        )

    seconds_since_heartbeat = (
        now - observation.last_collector_heartbeat
    ).total_seconds()
    if seconds_since_heartbeat >= 20 * 60:
        return GuardianDecision(
            "WAKE_COLLECTOR", "SLEEPING", False, "Collector heartbeat is stale"
        )

    return GuardianDecision(
        "NO_ACTION", "HEALTHY", False, "Collector is progressing"
    )
