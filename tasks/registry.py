"""
Task type registry.

Central registry for all available task types. Tasks register themselves
here so the dispatcher and workers can look them up by name.
"""

import logging
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)


class TaskRegistry:
    """Registry mapping task type names to their handler functions."""

    _instance: Optional["TaskRegistry"] = None
    _handlers: Dict[str, Callable] = {}
    _metadata: Dict[str, dict] = {}

    def __new__(cls) -> "TaskRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._handlers = {}
            cls._instance._metadata = {}
        return cls._instance

    def register(
        self,
        task_type: str,
        handler: Callable,
        description: str = "",
        estimated_duration: float = 0.0,
        resource_class: str = "default",
    ) -> None:
        """Register a task handler for a given task type."""
        self._handlers[task_type] = handler
        self._metadata[task_type] = {
            "description": description,
            "estimated_duration": estimated_duration,
            "resource_class": resource_class,
        }
        logger.info("Registered task type: %s (%s)", task_type, resource_class)

    def get_handler(self, task_type: str) -> Optional[Callable]:
        """Return the handler for a task type, or None if not found."""
        return self._handlers.get(task_type)

    def get_metadata(self, task_type: str) -> Optional[dict]:
        """Return metadata for a task type."""
        return self._metadata.get(task_type)

    def list_task_types(self) -> list:
        """List all registered task types with metadata."""
        return [
            {"task_type": name, **meta}
            for name, meta in self._metadata.items()
        ]

    def is_registered(self, task_type: str) -> bool:
        """Check if a task type is registered."""
        return task_type in self._handlers

    def unregister(self, task_type: str) -> bool:
        """Remove a task type from the registry."""
        if task_type in self._handlers:
            del self._handlers[task_type]
            del self._metadata[task_type]
            return True
        return False

    def clear(self) -> None:
        """Clear all registered handlers (primarily for testing)."""
        self._handlers.clear()
        self._metadata.clear()


# Module-level convenience
registry = TaskRegistry()


def register_task(
    task_type: str,
    description: str = "",
    estimated_duration: float = 0.0,
    resource_class: str = "default",
):
    """Decorator to register a function as a task handler."""

    def decorator(func: Callable) -> Callable:
        registry.register(
            task_type=task_type,
            handler=func,
            description=description,
            estimated_duration=estimated_duration,
            resource_class=resource_class,
        )
        return func

    return decorator
