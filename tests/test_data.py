from __future__ import annotations

import json
import os

import pandas as pd
import pytest

from spark_tunning_ml.data import Data

CSV_TEST = 'test.csv'
JSON_TEST = 'test.json'


@pytest.fixture
def data_instance():
    return Data()


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
    data_list = 'not a list'
    json_file = JSON_TEST

    # Act and Assert
    with pytest.raises(TypeError):
        data_instance.list_to_json(data_list, json_file)


def test_create_folders_valid_input(data_instance):
    folder_paths = ['path1', 'path2', 'path3']
    assert data_instance.create_folders(folder_paths) is None
    # Add assertions to check if the folders are created as expected


def test_create_folders_empty_list(data_instance):
    folder_paths = []
    with pytest.raises(ValueError):
        data_instance.create_folders(folder_paths)


def test_create_folders_invalid_input(data_instance):
    folder_paths = 'path'
    with pytest.raises(TypeError):
        data_instance.create_folders(folder_paths)


# Test case 2: Test when the input directory does not exist
def test_compact_parquet_files_with_nonexistent_directory(data_instance):
    input_directory = '/path/to/nonexistent_directory'
    output_file = '/tmp/output_file.parquet'

    # Call the function and assert that it raises a FileNotFoundError
    with pytest.raises(FileNotFoundError):
        data_instance.compact_parquet_files(input_directory, output_file)


# Utility function to create dummy Parquet files in a directory
def create_dummy_parquet_files(directory):
    # Create some dummy data
    data = {'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']}
    df = pd.DataFrame(data)

    # Save the data as Parquet files in the directory
    for i in range(3):
        file_path = os.path.join(directory, f'file_{i}.parquet')
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
        tmp_path / 'data_1.parquet',
        tmp_path / 'data_2.parquet',
        tmp_path / 'other_file.txt',  # This file should not be deleted
    ]
    for file_path in file_paths:
        with open(file_path, 'w') as f:
            f.write('Sample data')
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


def test_remove_directory(tmpdir, data_instance):
    """
    Test the remove_directory function.

    Parameters:
    - tmpdir: A pytest fixture for creating temporary directories.

    Returns:
    - None
    """
    # Create a temporary directory for testing
    test_dir = tmpdir.mkdir('test_directory')

    # Create some files and subdirectories in the test directory
    test_file = test_dir.join('test_file.txt')
    test_file.write('This is a test file.')

    subdirectory = test_dir.mkdir('subdirectory')
    subdirectory_file = subdirectory.join('sub_file.txt')
    subdirectory_file.write('This is a file in the subdirectory.')

    # Call the remove_directory function
    data_instance.remove_directory(str(test_dir))

    # Check that the directory and its contents are removed
    assert not os.path.exists(test_dir)
    assert not os.path.exists(test_file)
    assert not os.path.exists(subdirectory)
    assert not os.path.exists(subdirectory_file)


@pytest.fixture
def test_directory(tmp_path):
    dir_path = tmp_path / 'test_directory'
    dir_path.mkdir()

    # Create some files and subdirectories
    file_names = ['file1.txt', 'file2.txt', 'file3.md', 'subdir/file4.txt']
    for file_name in file_names:
        file_path = dir_path / file_name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch()

    return str(dir_path)


def test_count_files_all_files(test_directory, data_instance):
    total_files = data_instance.count_files(test_directory)
    assert total_files == 4  # 4 files were created in the test directory


def test_count_files_filtered_by_extension(test_directory, data_instance):
    total_files_txt = data_instance.count_files(
        test_directory, extension='txt')
    assert total_files_txt == 3  # 3 txt files were created in the test directory

    total_files_md = data_instance.count_files(test_directory, extension='md')
    assert total_files_md == 1  # 1 md file was created in the test directory


def test_count_files_nonexistent_extension(test_directory, data_instance):
    total_files = data_instance.count_files(test_directory, extension='png')
    assert total_files == 0
