# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


EXECUTE_ON_ARRIVAL = "arrival"
EXECUTE_ON_DISPATCH = "dispatch"
EXECUTE_ON_REVERTING = "reverting"
EXECUTE_ON_DEFAULT = "default"


@dataclass(slots=True)
class ItineraryPlan:
    """Serializable itinerary state for a mobile paglet.

    The Java helpers increment their internal index after calling dispatch.
    In paglets the serializable state is captured during dispatch, so this
    Python helper advances before dispatching to ensure the arrived copy sees
    the updated route position.
    """

    destinations: list[str] = field(default_factory=list)
    current_index: int = 0
    current_location: str | None = None
    visited_destinations: list[str] = field(default_factory=list)
    mutable: bool = True
    circular: bool = False
    loop_count: int = 0
    completed: bool = False

    def add_next_destination(
        self,
        destination: str,
        *,
        index: int | None = None,
        after: str | None = None,
        allow_duplicate: bool = True,
    ) -> bool:
        if not self.mutable or not destination:
            return False
        if not allow_duplicate and destination in self.destinations:
            return False
        if after is not None:
            try:
                index = self.destinations.index(after) + 1
            except ValueError:
                self.destinations.append(after)
                index = len(self.destinations)
        if index is None:
            self.destinations.append(destination)
            return True
        if index < 0 or index > len(self.destinations):
            return False
        self.destinations.insert(index, destination)
        return True

    def add_next_destination_if_not_duplicated(self, destination: str) -> bool:
        return self.add_next_destination(destination, allow_duplicate=False)

    def remove_destination(self, destination: str) -> bool:
        if not self.mutable or destination not in self.destinations:
            return False
        self.destinations.remove(destination)
        self.current_index = min(self.current_index, len(self.destinations))
        return True

    def remove_destination_at(self, index: int) -> bool:
        if not self.mutable or index < 0 or index >= len(self.destinations):
            return False
        self.destinations.pop(index)
        self.current_index = min(self.current_index, len(self.destinations))
        return True

    def set_immutable(self) -> None:
        self.mutable = False

    def skip_next(self) -> None:
        if self.current_index < len(self.destinations):
            self.current_index += 1
        elif self.circular and self.destinations:
            self.current_index = 0
            self.loop_count += 1

    def get_destination_count(self) -> int:
        return len(self.destinations)

    def get_remaining_destination_count(self) -> int:
        return max(0, len(self.destinations) - self.current_index)

    def get_destinations(self) -> list[str]:
        return list(self.destinations)

    def get_first_destination(self) -> str | None:
        return self.destinations[0] if self.destinations else None

    def get_last_destination(self) -> str | None:
        return self.destinations[-1] if self.destinations else None

    def get_current_location(self) -> str | None:
        return self.current_location

    def get_loop_count(self) -> int:
        return self.loop_count

    def next_destination(self) -> str | None:
        if self.current_index < len(self.destinations):
            return self.destinations[self.current_index]
        if self.circular and self.destinations:
            return self.destinations[0]
        return None

    def dispatch_next(self, agent: Any) -> Any | None:
        target = self.next_destination()
        if target is None:
            self.completed = True
            return None
        if self.current_index >= len(self.destinations):
            self.current_index = 0
            self.loop_count += 1
        self.current_index += 1
        self.current_location = target
        self.visited_destinations.append(target)
        return agent.dispatch(target)


@dataclass(slots=True)
class ItineraryTask:
    """Serializable task descriptor for a task itinerary."""

    name: str
    execution: str = EXECUTE_ON_ARRIVAL
    args: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TaskItineraryPlan(ItineraryPlan):
    destination_tasks: dict[str, list[ItineraryTask]] = field(default_factory=dict)
    default_tasks: list[ItineraryTask] = field(default_factory=list)
    execute_default_on_arrival: bool = True
    execute_default_on_dispatch: bool = False
    execute_default_on_reverting: bool = False

    def add_default_task(self, task: ItineraryTask) -> bool:
        if task.execution != EXECUTE_ON_DEFAULT or task in self.default_tasks:
            return False
        self.default_tasks.append(task)
        return True

    def remove_default_task(self, task: ItineraryTask) -> bool:
        if task not in self.default_tasks:
            return False
        self.default_tasks.remove(task)
        return True

    def add_task_for_destination(self, destination: str, task: ItineraryTask) -> bool:
        if not destination:
            return False
        if task.execution == EXECUTE_ON_DEFAULT:
            return self.add_default_task(task)
        self.destination_tasks.setdefault(destination, []).append(task)
        return True

    def add_next_destination_with_task(self, destination: str, task: ItineraryTask) -> bool:
        added = self.add_next_destination(destination)
        if added:
            self.add_task_for_destination(destination, task)
        return added

    def remove_all_tasks_for_destination(self, destination: str) -> bool:
        return self.destination_tasks.pop(destination, None) is not None

    def remove_task_for_destination(self, destination: str, task: ItineraryTask) -> bool:
        tasks = self.destination_tasks.get(destination)
        if not tasks or task not in tasks:
            return False
        tasks.remove(task)
        if not tasks:
            self.destination_tasks.pop(destination, None)
        return True

    def get_tasks_for_destination(self, destination: str) -> list[ItineraryTask]:
        return list(self.destination_tasks.get(destination, []))

    def tasks_for_phase(self, destination: str | None, phase: str) -> list[ItineraryTask]:
        tasks: list[ItineraryTask] = []
        if self._default_enabled_for_phase(phase):
            tasks.extend(self.default_tasks)
        if destination is not None:
            tasks.extend(
                task
                for task in self.destination_tasks.get(destination, [])
                if task.execution == phase
            )
        return tasks

    def _default_enabled_for_phase(self, phase: str) -> bool:
        if phase == EXECUTE_ON_ARRIVAL:
            return self.execute_default_on_arrival
        if phase == EXECUTE_ON_DISPATCH:
            return self.execute_default_on_dispatch
        if phase == EXECUTE_ON_REVERTING:
            return self.execute_default_on_reverting
        return False


class ItineraryAgentMixin:
    """Mixin for paglets whose state stores an ``itinerary`` field."""

    itinerary_attr = "itinerary"

    def get_itinerary(self) -> ItineraryPlan:
        return getattr(self.state, self.itinerary_attr)

    def go_to_next_destination(self) -> Any | None:
        return self.get_itinerary().dispatch_next(self)

    def execute_itinerary_tasks(self, phase: str, event: Any = None) -> list[Any]:
        itinerary = self.get_itinerary()
        if not isinstance(itinerary, TaskItineraryPlan):
            return []
        tasks = itinerary.tasks_for_phase(itinerary.get_current_location(), phase)
        return [self.execute_itinerary_task(task, phase, event) for task in tasks]

    def execute_itinerary_task(self, task: ItineraryTask, phase: str, event: Any = None) -> Any:
        raise NotImplementedError(f"{self.__class__.__name__} must handle itinerary task {task.name!r}")
