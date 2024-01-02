from __future__ import annotations

from unittest.mock import patch

import pytest
import requests

from spark_tunning_ml.request_handler import (
    RequestHandler,
)


@pytest.fixture
def request_wrapper():
    return RequestHandler("https://api.example.com")


@patch("requests.get")
def test_get_request(mock_get, request_wrapper):
    # Mocking the response for the GET request
    endpoint = "get_endpoint"
    expected_url = f"https://api.example.com{endpoint}"
    expected_response_json = {"key": "value"}
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response.json = lambda: expected_response_json
    mock_get.return_value = mock_response

    # Making the actual GET request
    response_json = request_wrapper.request("GET", endpoint)

    mock_get.assert_called_once_with(expected_url, params=None)
    assert response_json == expected_response_json


@patch("requests.post")
def test_post_request(mock_post, request_wrapper):
    # Mocking the response for the POST request
    endpoint = "post_endpoint"
    expected_url = f"https://api.example.com{endpoint}"
    expected_response_json = {"key": "value"}
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response.json = lambda: expected_response_json
    mock_post.return_value = mock_response

    # Making the actual POST request
    response_json = request_wrapper.request("POST", endpoint)

    mock_post.assert_called_once_with(expected_url, data=None, json=None)
    assert response_json == expected_response_json


def test_invalid_method(request_wrapper):
    # Testing invalid HTTP method
    with pytest.raises(ValueError, match="Unsupported HTTP method. Supported methods: 'GET', 'POST'."):
        request_wrapper.request("INVALID_METHOD", "some_endpoint")
