from __future__ import annotations

import jsonschema
import pytest

from spark_tunning_ml.schema import SchemaValidator

# Example schema for testing
example_schema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer"},
    },
    "required": ["name", "age"],
}


@pytest.fixture
def my_schema():
    return SchemaValidator(example_schema)


def test_validate_success(my_schema):
    # Example JSON data that conforms to the schema
    valid_data = {"name": "John Doe", "age": 30}

    # No exception should be raised
    my_schema.validate(valid_data)


def test_validate_failure(my_schema):
    # Example JSON data that does not conform to the schema
    invalid_data = {"name": "John Doe"}  # Missing "age" field

    # ValidationError should be raised
    with pytest.raises(jsonschema.exceptions.ValidationError):
        my_schema.validate(invalid_data)


def test_iter_errors(my_schema):
    # Example JSON data that does not conform to the schema
    invalid_data = {"name": "John Doe"}  # Missing "age" field

    # Getting an iterator of validation errors
    errors_iterator = my_schema.iter_errors(invalid_data)

    # At least one error should be present in the iterator
    assert any(errors_iterator)


def test_validate_with_schema_error():
    # Example schema with an error
    invalid_schema = {"type": "objec"}  # Missing 't' in 'object'

    # Creating SchemaValidator instance with the invalid schema
    my_schema = SchemaValidator(invalid_schema)

    # SchemaError should be raised during initialization
    with pytest.raises(jsonschema.exceptions.SchemaError):
        my_schema.validate({})
