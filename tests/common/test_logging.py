"""Tests for common logging."""

import pytest
import json

from src.common.logging import setup_logging, get_logger


class TestLoggingSetup:
    """Tests for logging setup."""

    def test_setup_logging_json_format(self):
        """Test logging setup with JSON format."""
        logger = setup_logging(level="INFO", format_type="json", output="stdout")
        assert logger is not None

    def test_setup_logging_console_format(self):
        """Test logging setup with console format."""
        logger = setup_logging(level="DEBUG", format_type="console", output="stdout")
        assert logger is not None

    def test_get_logger_without_name(self):
        """Test getting a logger without a name."""
        setup_logging()
        logger = get_logger()
        assert logger is not None

    def test_get_logger_with_name(self):
        """Test getting a logger with a name."""
        setup_logging()
        logger = get_logger("test_logger")
        assert logger is not None

    def test_get_logger_with_context(self):
        """Test getting a logger with initial context."""
        setup_logging()
        logger = get_logger("context_logger", user="test_user", request_id="12345")
        assert logger is not None

    def test_log_message_with_context(self):
        """Test logging a message with context."""
        setup_logging(level="INFO", format_type="json")
        logger = get_logger("message_test", operation="test")

        # This should not raise an exception
        logger.info("Test message", extra_key="extra_value")


class TestLoggerFunctions:
    """Tests for logger functions."""

    def test_logger_debug(self):
        """Test logging at DEBUG level."""
        setup_logging(level="DEBUG")
        logger = get_logger("debug_test")
        logger.debug("Debug message")

    def test_logger_info(self):
        """Test logging at INFO level."""
        setup_logging(level="INFO")
        logger = get_logger("info_test")
        logger.info("Info message")

    def test_logger_warning(self):
        """Test logging at WARNING level."""
        setup_logging(level="WARNING")
        logger = get_logger("warning_test")
        logger.warning("Warning message")

    def test_logger_error(self):
        """Test logging at ERROR level."""
        setup_logging(level="ERROR")
        logger = get_logger("error_test")
        logger.error("Error message")
