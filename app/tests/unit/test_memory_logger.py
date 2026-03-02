"""Unit tests for periodic memory logger utilities."""

from __future__ import annotations

import logging

from app.logging_setup import PeriodicMemoryLogger


def test_memory_logger_log_once_emits_record(caplog) -> None:
    logger = logging.getLogger("test.memory")
    memory_logger = PeriodicMemoryLogger(interval_seconds=0.1, logger=logger)
    memory_logger.start()

    with caplog.at_level(logging.INFO):
        memory_logger.log_once()

    memory_logger.stop()

    assert any(record.message == "memory_usage" for record in caplog.records)
