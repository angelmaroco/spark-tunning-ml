import unittest
import logging
from unittest.mock import patch
from spark_tunning_ml.logger import Logger

class LoggerTests(unittest.TestCase):
    def test_info_logging(self):
        logger = Logger()
        with patch('logging.Logger.info') as mock_info:
            logger.info('Test message')
            mock_info.assert_called_with('Test message')

    def test_warning_logging(self):
        logger = Logger()
        with patch('logging.Logger.warning') as mock_warning:
            logger.warning('Test message')
            mock_warning.assert_called_with('Test message')

    def test_error_logging(self):
        logger = Logger()
        with patch('logging.Logger.error') as mock_error:
            logger.error('Test message')
            mock_error.assert_called_with('Test message')

    def test_critical_logging(self):
        logger = Logger()
        with patch('logging.Logger.critical') as mock_critical:
            logger.critical('Test message')
            mock_critical.assert_called_with('Test message')

    def test_debug_logging(self):
        logger = Logger()
        with patch('logging.Logger.debug') as mock_debug:
            logger.debug('Test message')
            mock_debug.assert_called_with('Test message')