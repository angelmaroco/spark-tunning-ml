import os
import pytest
import json
from spark_tunning_ml.data import Data
import pandas as pd

CSV_TEST = "test.csv"
JSON_TEST = "test.json"


@pytest.fixture
def data_instance():
    return Data()


def test_no_add_column_value_no_select_columns(data_instance):
    # Test converting JSON to Parquet without adding a new column and without selecting specific columns
    data_list = [{"name": "John", "age": 25}, {"name": "Jane", "age": 30}]
    parquet_file = "output.parquet"

    assert data_instance.json_to_parquet(data_list, parquet_file) is None
    # Assert that the Parquet file is created successfully


def test_no_add_column_value_with_select_columns(data_instance):
    # Test converting JSON to Parquet without adding a new column and with selecting specific columns
    data_list = [
        {"name": "John", "age": 25, "city": "New York"},
        {"name": "Jane", "age": 30, "city": "Los Angeles"},
    ]
    parquet_file = "output.parquet"
    select_columns = ["name", "age"]

    assert (
        data_instance.json_to_parquet(
            data_list, parquet_file, select_columns=select_columns
        )
        is None
    )
    # Assert that the Parquet file is created successfully and contains only the selected columns


def test_with_add_column_value_no_select_columns(data_instance):
    # Test converting JSON to Parquet with adding a new column and without selecting specific columns
    data_list = [{"name": "John", "age": 25}, {"name": "Jane", "age": 30}]
    parquet_file = "output.parquet"
    add_column_value = {"city": "New York"}

    assert (
        data_instance.json_to_parquet(
            data_list, parquet_file, add_column_value=add_column_value
        )
        is None
    )
    # Assert that the Parquet file is created successfully and contains the added column


def test_with_add_column_value_with_select_columns(data_instance):
    # Test converting JSON to Parquet with adding a new column and with selecting specific columns
    data_list = [
        {"name": "John", "age": 25, "city": "New York"},
        {"name": "Jane", "age": 30, "city": "Los Angeles"},
    ]
    parquet_file = "output.parquet"
    add_column_value = {"country": "USA"}
    select_columns = ["name", "age"]

    assert (
        data_instance.json_to_parquet(
            data_list,
            parquet_file,
            add_column_value=add_column_value,
            select_columns=select_columns,
        )
        is None
    )
    # Assert that the Parquet file is created successfully, contains the added column, and contains only the selected columns


def test_list_to_json_with_valid_data(data_instance):
    # Arrange
    data_list = [1, 2, 3]
    json_file = JSON_TEST

    # Act
    data_instance.list_to_json(data_list, json_file)

    # Assert
    with open(json_file) as file:
        result = json.load(file)
    assert result == data_list


def test_list_to_json_with_empty_list(data_instance):
    # Arrange
    data_list = []
    json_file = JSON_TEST

    # Act and Assert
    with pytest.raises(ValueError):
        data_instance.list_to_json(data_list, json_file)


def test_list_to_json_with_invalid_data(data_instance):
    # Arrange
    data_list = "not a list"
    json_file = JSON_TEST

    # Act and Assert
    with pytest.raises(TypeError):
        data_instance.list_to_json(data_list, json_file)


def test_create_folders_valid_input(data_instance):
    folder_paths = ["path1", "path2", "path3"]
    assert data_instance.create_folders(folder_paths) is None
    # Add assertions to check if the folders are created as expected


def test_create_folders_empty_list(data_instance):
    folder_paths = []
    with pytest.raises(ValueError):
        data_instance.create_folders(folder_paths)


def test_create_folders_invalid_input(data_instance):
    folder_paths = "path"
    with pytest.raises(TypeError):
        data_instance.create_folders(folder_paths)

# Test case 2: Test when the input directory does not exist
def test_compact_parquet_files_with_nonexistent_directory(data_instance):
    input_directory = "/path/to/nonexistent_directory"
    output_file = "/tmp/output_file.parquet"

    # Call the function and assert that it raises a FileNotFoundError
    with pytest.raises(FileNotFoundError):
        data_instance.compact_parquet_files(input_directory, output_file)

# Utility function to create dummy Parquet files in a directory
def create_dummy_parquet_files(directory):
    # Create some dummy data
    data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    df = pd.DataFrame(data)

    # Save the data as Parquet files in the directory
    for i in range(3):
        file_path = os.path.join(directory, f"file_{i}.parquet")
        df.to_parquet(file_path, index=False)


# Utility function to check if a file is a valid Parquet file
def is_valid_parquet_file(file_path):
    try:
        pd.read_parquet(file_path)
        return True
    except:
        return False

@pytest.fixture
def sample_files(tmp_path):
    # Create sample Parquet files in a temporary directory
    file_paths = [
        tmp_path / "data_1.parquet",
        tmp_path / "data_2.parquet",
        tmp_path / "other_file.txt",  # This file should not be deleted
    ]
    for file_path in file_paths:
        with open(file_path, "w") as f:
            f.write("Sample data")
    return tmp_path

def test_delete_files(sample_files, data_instance):
    # Test deleting Parquet files with the filter 'data_*'
    directory = sample_files
    file_filter = 'data_*'
    
    # Call the function
    deleted_count = data_instance.delete_files(directory, file_filter)

    # Check if the correct number of files were deleted
    assert deleted_count == 2

    # Check if only files were deleted
    remaining_files = os.listdir(directory)
    assert all(file.endswith('.parquet') is False for file in remaining_files)