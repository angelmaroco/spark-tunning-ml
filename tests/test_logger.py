from __future__ import annotations

import unittest
from unittest.mock import patch

from spark_tunning_ml.logger import Logger

TEST_MESSAGE = 'Test message'


class LoggerTests(unittest.TestCase):
    def test_info_logging(self):
        logger = Logger()
        with patch('logging.Logger.info') as mock_info:
            logger.info(TEST_MESSAGE)
            mock_info.assert_called_with(TEST_MESSAGE)

    def test_warning_logging(self):
        logger = Logger()
        with patch('logging.Logger.warning') as mock_warning:
            logger.warning(TEST_MESSAGE)
            mock_warning.assert_called_with(TEST_MESSAGE)

    def test_error_logging(self):
        logger = Logger()
        with patch('logging.Logger.error') as mock_error:
            logger.error(TEST_MESSAGE)
            mock_error.assert_called_with(TEST_MESSAGE)

    def test_critical_logging(self):
        logger = Logger()
        with patch('logging.Logger.critical') as mock_critical:
            logger.critical(TEST_MESSAGE)
            mock_critical.assert_called_with(TEST_MESSAGE)

    def test_debug_logging(self):
        logger = Logger()
        with patch('logging.Logger.debug') as mock_debug:
            logger.debug(TEST_MESSAGE)
            mock_debug.assert_called_with(TEST_MESSAGE)
