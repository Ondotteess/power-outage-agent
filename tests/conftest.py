from __future__ import annotations

from dataclasses import dataclass, field

from app.workers.queue import Task


@dataclass
class FakeTaskStore:
    """In-memory stub for TaskStoreProtocol used by Dispatcher tests."""

    calls: list[tuple[Task, str, str | None]] = field(default_factory=list)

    async def upsert(self, task: Task, status: str, error: str | None = None) -> None:
        self.calls.append((task, status, error))

    def statuses_for(self, task_id) -> list[str]:
        return [s for (t, s, _e) in self.calls if t.task_id == task_id]
