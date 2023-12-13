from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch

import jsonschema
import pytest

from spark_tunning_ml.spark_ui_wrapper import (
    SparkUIWrapper,
)


@pytest.fixture
def spark_ui_wrapper():
    return SparkUIWrapper('https://api.example.com')


@patch('requests.get')
def test_successful_get_applications(mock_get, spark_ui_wrapper):
    # Mocking the response for the GET request
    endpoint = '/applications?status=running&limit=10000'
    expected_url = f'https://api.example.com{endpoint}'
    expected_response_json = [
        {
            'id': 'app-20231204142916-0004',
            'name': 'Python Spark SQL data source example',
            'attempts': [
                {
                    'startTime': '2023-12-04T14:29:14.422GMT',
                    'endTime': '1969-12-31T23:59:59.999GMT',
                    'lastUpdated': '2023-12-04T14:29:14.422GMT',
                    'duration': 0,
                    'sparkUser': 'root',
                    'completed': False,
                    'appSparkVersion': '2.4.1',
                    'startTimeEpoch': 1701700154422,
                    'endTimeEpoch': -1,
                    'lastUpdatedEpoch': 1701700154422,
                    'attemptId': '1',  # Optional field
                },
            ],
        },
        {
            'id': 'app-20231204142916-0005',
            'name': 'Python Spark SQL data source example',
            'attempts': [
                {
                    'startTime': '2023-12-04T14:30:00.000GMT',
                    'endTime': '1969-12-31T23:59:59.999GMT',
                    'lastUpdated': '2023-12-04T14:30:00.000GMT',
                    'duration': 0,
                    'sparkUser': 'root',
                    'completed': True,
                    'appSparkVersion': '2.4.1',
                    'startTimeEpoch': 1701700200000,
                    'endTimeEpoch': -1,
                    'lastUpdatedEpoch': 1701700200000,
                    'attemptId': '2',  # Optional field
                },
                {
                    'startTime': '2023-12-04T14:30:30.000GMT',
                    'endTime': '1969-12-31T23:59:59.999GMT',
                    'lastUpdated': '2023-12-04T14:30:30.000GMT',
                    'duration': 0,
                    'sparkUser': 'root',
                    'completed': True,
                    'appSparkVersion': '2.4.1',
                    'startTimeEpoch': 1701700230000,
                    'endTimeEpoch': -1,
                    'lastUpdatedEpoch': 1701700230000,
                    'attemptId': '1',  # Optional field
                },
            ],
        },
    ]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = expected_response_json
    mock_get.return_value = mock_response

    # Making the actual GET request
    response = spark_ui_wrapper.get_applications(apps_limit=10000)

    mock_get.assert_called_once_with(expected_url, params=None)
    assert response == expected_response_json


@patch('requests.get')
def test_failed_get_applications(mock_get, spark_ui_wrapper):
    # Mocking the response for the GET request
    endpoint = '/applications?status=running&limit=10000'
    expected_url = f'https://api.example.com{endpoint}'
    # Invalid response for testing failure
    invalid_response_json = {'invalid_key': 'value'}
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = invalid_response_json
    mock_get.return_value = mock_response

    with pytest.raises(jsonschema.exceptions.ValidationError):
        spark_ui_wrapper.get_applications()

    mock_get.assert_called_once_with(expected_url, params=None)


@pytest.mark.parametrize(
    'applications, expected_ids',
    [
        (
            [
                {
                    'id': 'app-20231204142916-0004',
                    'name': 'Python Spark SQL data source example',
                    'attempts': [
                        {
                            'startTime': '2023-12-04T14:29:14.422GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:29:14.422GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': False,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700154422,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700154422,
                        },
                    ],
                },
                {
                    'id': 'app-20231204142916-0005',
                    'name': 'Python Spark SQL data source example',
                    'attempts': [
                        {
                            'startTime': '2023-12-04T14:30:00.000GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:30:00.000GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700200000,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700200000,
                            'attemptId': '2',  # Optional field
                        },
                        {
                            'startTime': '2023-12-04T14:30:30.000GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:30:30.000GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700230000,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700230000,
                        },
                    ],
                },
            ],
            [{'app-20231204142916-0004': 0}, {'app-20231204142916-0005': 2}],
        ),
    ],
)
def test_get_ids_from_applications_case1(applications, expected_ids, spark_ui_wrapper):
    ids = spark_ui_wrapper.get_ids_from_applications(
        applications,
    )
    print(ids, expected_ids)
    assert ids == expected_ids


@pytest.mark.parametrize(
    'applications, expected_ids',
    [
        (
            [
                {
                    'id': 'app-20231204142916-0004',
                    'name': 'Python Spark SQL data source example',
                    'attempts': [
                        {
                            'startTime': '2023-12-04T14:29:14.422GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:29:14.422GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700154422,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700154422,
                            'attemptId': '1',  # Optional field
                        },
                    ],
                },
                {
                    'id': 'app-20231204142916-0005',
                    'name': 'Python Spark SQL data source example',
                    'attempts': [
                        {
                            'startTime': '2023-12-04T14:30:00.000GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:30:00.000GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700200000,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700200000,
                            'attemptId': '2',  # Optional field
                        },
                        {
                            'startTime': '2023-12-04T14:30:30.000GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:30:30.000GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700230000,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700230000,
                            'attemptId': '1',  # Optional field
                        },
                    ],
                },
            ],
            [{'app-20231204142916-0004': 1}, {'app-20231204142916-0005': 2}],
        ),
    ],
)
def test_get_ids_from_applications_case2(applications, expected_ids, spark_ui_wrapper):
    ids = spark_ui_wrapper.get_ids_from_applications(
        applications,
    )
    assert ids == expected_ids


@pytest.mark.parametrize(
    'applications, expected_ids',
    [
        (
            [
                {
                    'id': 'app-20231204142916-0004',
                    'name': 'Python Spark SQL data source example',
                    'attempts': [
                        {
                            'startTime': '2023-12-04T14:29:14.422GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:29:14.422GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700154422,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700154422,
                            'attemptId': '1',  # Optional field
                        },
                    ],
                },
                {
                    'id': 'app-20231204142916-0005',
                    'name': 'Python Spark SQL data source example',
                    'attempts': [
                        {
                            'startTime': '2023-12-04T14:30:00.000GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:30:00.000GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700200000,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700200000,
                            'attemptId': '1',  # Optional field
                        },
                        {
                            'startTime': '2023-12-04T14:30:30.000GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:30:30.000GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700230000,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700230000,
                            'attemptId': '2',  # Optional field
                        },
                        {
                            'startTime': '2023-12-04T14:30:30.000GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:30:30.000GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700230000,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700230000,
                            'attemptId': '3',  # Optional field
                        },
                    ],
                },
            ],
            [{'app-20231204142916-0004': 1}, {'app-20231204142916-0005': 3}],
        ),
    ],
)
def test_get_ids_from_applications_case3(applications, expected_ids, spark_ui_wrapper):
    ids = spark_ui_wrapper.get_ids_from_applications(
        applications,
    )
    assert ids == expected_ids


@pytest.mark.parametrize(
    'applications, expected_ids',
    [
        (
            [
                {
                    'id': 'app-20231204142916-00041',
                    'name': 'Python Spark SQL data source example',
                    'attempts': [
                        {
                            'startTime': '2023-12-04T14:29:14.422GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:29:14.422GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700154422,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700154422,
                            'attemptId': '1',  # Optional field
                        },
                    ],
                },
                {
                    'id': 'app-20231204142916-00051',
                    'name': 'Python Spark SQL data source example',
                    'attempts': [
                        {
                            'startTime': '2023-12-04T14:30:00.000GMT',
                            'endTime': '1969-12-31T23:59:59.999GMT',
                            'lastUpdated': '2023-12-04T14:30:00.000GMT',
                            'duration': 0,
                            'sparkUser': 'root',
                            'completed': True,
                            'appSparkVersion': '2.4.1',
                            'startTimeEpoch': 1701700200000,
                            'endTimeEpoch': -1,
                            'lastUpdatedEpoch': 1701700200000,
                        },
                    ],
                },
            ],
            [{'app-20231204142916-00041': 1}, {'app-20231204142916-00051': 0}],
        ),
    ],
)
def test_get_ids_from_applications_case5(applications, expected_ids, spark_ui_wrapper):
    ids = spark_ui_wrapper.get_ids_from_applications(
        applications,
    )
    print(ids, expected_ids)
    assert ids == expected_ids


def test_get_id_from_stage_attempts(spark_ui_wrapper):
    # Test case 1: stage is an empty list
    stage = []
    result = spark_ui_wrapper.get_id_from_stage_attempts(stage)
    assert result is None

    # Test case 2: stage is a list with a single dictionary
    stage = [{'attemptId': 123}]
    result = spark_ui_wrapper.get_id_from_stage_attempts(stage)
    assert result == 123
