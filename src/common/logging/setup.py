"""Structured logging setup using structlog."""

import logging
import sys
from typing import Any, Optional

import structlog


def setup_logging(
    level: str = "INFO",
    format_type: str = "json",
    output: str = "stdout",
) -> structlog.BoundLogger:
    """Setup structured logging with structlog.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: Output format ('json' or 'console')
        output: Output destination ('stdout', 'stderr', or file path)

    Returns:
        A configured structlog logger
    """
    # Configure logging level
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Configure processors
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # Add format processor based on format_type
    if format_type == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Console format - use a more readable format
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
            )
        )

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(
            file=sys.stdout if output == "stdout" else sys.stderr
        ),
        cache_logger_on_first_use=True,
    )

    # Also configure the standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout if output == "stdout" else sys.stderr,
        level=log_level,
    )

    return structlog.get_logger()


def get_logger(name: Optional[str] = None, **initial_context: Any) -> structlog.BoundLogger:
    """Get a configured logger instance.

    Args:
        name: Optional logger name
        **initial_context: Initial context to bind to the logger

    Returns:
        A configured structlog logger
    """
    logger = structlog.get_logger(name) if name else structlog.get_logger()

    if initial_context:
        return logger.bind(**initial_context)

    return logger
