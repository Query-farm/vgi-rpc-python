"""Using dataclasses as RPC parameters and return types.

``ArrowSerializableDataclass`` lets you pass structured data across the
RPC boundary. Fields are automatically mapped to Arrow types.

Run::

    python examples/structured_types.py
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from vgi_rpc import serve_pipe
from vgi_rpc.utils import ArrowSerializableDataclass

# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class Priority(Enum):
    """Task priority levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass(frozen=True)
class Task(ArrowSerializableDataclass):
    """A task with structured fields.

    Supported field types include: str, int, float, bool, Enum,
    list[T], dict[K, V], frozenset[T], Optional types, and nested
    ArrowSerializableDataclass instances.
    """

    title: str
    priority: Priority
    tags: list[str]
    metadata: dict[str, str]
    done: bool = False


@dataclass(frozen=True)
class TaskSummary(ArrowSerializableDataclass):
    """Summary returned by the task service."""

    total: int
    high_priority: int
    titles: list[str]


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class TaskService(Protocol):
    """Service that accepts and returns dataclass parameters."""

    def create_task(self, task: Task) -> str:
        """Store a task and return its ID."""
        ...

    def summarize(self) -> TaskSummary:
        """Return a summary of all stored tasks."""
        ...


class TaskServiceImpl:
    """In-memory task store."""

    def __init__(self) -> None:
        """Initialize the store."""
        self._tasks: list[Task] = []
        self._next_id: int = 0

    def create_task(self, task: Task) -> str:
        """Store a task and return its ID."""
        self._tasks.append(task)
        task_id = f"TASK-{self._next_id}"
        self._next_id += 1
        return task_id

    def summarize(self) -> TaskSummary:
        """Return a summary of all stored tasks."""
        return TaskSummary(
            total=len(self._tasks),
            high_priority=sum(1 for t in self._tasks if t.priority == Priority.HIGH),
            titles=[t.title for t in self._tasks],
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the dataclass example."""
    with serve_pipe(TaskService, TaskServiceImpl()) as svc:
        # Create some tasks using structured dataclass parameters
        id1 = svc.create_task(
            task=Task(
                title="Write documentation",
                priority=Priority.HIGH,
                tags=["docs", "urgent"],
                metadata={"assignee": "alice"},
            )
        )
        print(f"Created: {id1}")

        id2 = svc.create_task(
            task=Task(
                title="Run benchmarks",
                priority=Priority.LOW,
                tags=["perf"],
                metadata={"env": "staging"},
            )
        )
        print(f"Created: {id2}")

        # Get a structured summary back
        summary = svc.summarize()
        print(f"\nTotal tasks:    {summary.total}")
        print(f"High priority:  {summary.high_priority}")
        print(f"Titles:         {summary.titles}")


if __name__ == "__main__":
    main()
