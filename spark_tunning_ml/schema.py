import jsonschema
from jsonschema.exceptions import (FormatError, SchemaError, UnknownType,
                                   ValidationError)


class SchemaValidator:
    def __init__(self, schema):
        """
        Initialize the schema validator.

        Args:
            schema (dict): The JSON schema to validate against.
        """
        self.schema = schema
        self.validator = jsonschema.Draft7Validator(self.schema)

    def validate(self, instance):
        """
        Validate the JSON instance against the schema.

        Args:
            instance: The JSON data to validate.

        Raises:
            jsonschema.exceptions.ValidationError: If validation fails.
            jsonschema.exceptions.SchemaError: If there is an issue with the schema.
            jsonschema.exceptions.FormatError: If there is an issue with the format of the data.
            jsonschema.exceptions.UnknownType: If an unknown type is encountered in the schema.
        """
        try:
            jsonschema.validate(instance=instance, schema=self.schema)
        except (
            ValidationError,
            SchemaError,
            FormatError,
            UnknownType,
        ) as e:
            print(f"Validation failed: {e}")
            raise

    def iter_errors(self, instance):
        """
        Get an iterator of validation errors.

        Args:
            instance: The JSON data to validate.

        Returns:
            Iterator[jsonschema.exceptions.ValidationError]: Iterator of validation errors.
        """
        return self.validator.iter_errors(instance)
