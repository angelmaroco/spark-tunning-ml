import pytest
from unittest.mock import patch
from pymilvus import MilvusException
from spark_tunning_ml.milvus import MilvusWrapper


class TestConnect:
    def setup_method(self, method):
        self.your_instance = MilvusWrapper()

    @patch("pymilvus.connections.connect")
    def test_successful_connection(self, mock_connect):
        self.your_instance.connect()
        mock_connect.assert_called_once_with(host=self.your_instance.host, port=self.your_instance.port)

    @patch("pymilvus.connections.connect", side_effect=MilvusException("Error"))
    def test_failed_connection(self, mock_connect, capsys):
        self.your_instance.connect()
        captured = capsys.readouterr()
        assert captured.out == f"Failed to connect to Milvus: Error\n"
