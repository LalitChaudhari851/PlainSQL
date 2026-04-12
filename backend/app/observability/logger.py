"""
Structured Logger — JSON-formatted logging using structlog.
Provides consistent, machine-parseable log output for production monitoring.
"""

import sys
import logging
import structlog


def setup_logging(log_level: str = "INFO", json_output: bool = True):
    """
    Configure structured logging for the entire application.
    
    In production (json_output=True): JSON log lines for log aggregation (ELK, Datadog, etc.)
    In development (json_output=False): Colorized console output for readability.
    """
    # Set root log level
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )

    # Shared processors
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if json_output:
        # Production: JSON output
        renderer = structlog.processors.JSONRenderer()
    else:
        # Development: colorized console
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Also configure the formatter for stdlib logging
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    structlog.get_logger().info("logging_configured", level=log_level, json=json_output)
