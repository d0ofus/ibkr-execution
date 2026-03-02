"""Logging setup helpers."""

from __future__ import annotations

import logging
import threading
import tracemalloc


def setup_logging(level: str = "INFO") -> None:
    """Configure root logging for the process."""
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))


class PeriodicMemoryLogger:
    """Emit periodic process memory metrics using Python's tracemalloc."""

    def __init__(
        self,
        *,
        interval_seconds: float,
        logger: logging.Logger | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be > 0")

        self._interval_seconds = interval_seconds
        self._logger = logger or logging.getLogger("ibkr_exec.memory")
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start background periodic logging loop."""
        if self._thread is not None and self._thread.is_alive():
            return

        if not tracemalloc.is_tracing():
            tracemalloc.start()

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="memory-logger", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop background periodic logging loop."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None

    def log_once(self) -> None:
        """Emit one memory usage line."""
        current_bytes, peak_bytes = tracemalloc.get_traced_memory()
        self._logger.info(
            "memory_usage",
            extra={
                "memory_current_mb": round(current_bytes / (1024 * 1024), 3),
                "memory_peak_mb": round(peak_bytes / (1024 * 1024), 3),
            },
        )

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self.log_once()
            self._stop_event.wait(timeout=self._interval_seconds)


def create_periodic_memory_logger(interval_seconds: float) -> PeriodicMemoryLogger:
    """Construct a periodic memory logger instance."""
    return PeriodicMemoryLogger(interval_seconds=interval_seconds)
