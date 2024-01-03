from __future__ import annotations

import glob
import json
import os
import random
import shutil
import string
from datetime import datetime, timezone

import pandas as pd

from spark_tunning_ml.logger import logger


class Data:
    @staticmethod
    def dict_to_csv(data_dict, csv_file):
        """
        Write a dictionary to a CSV file.

        Parameters:
        - data_dict (dict): The input dictionary to be written to the CSV file.
        - csv_file (str): Path to the output CSV file.

        Raises:
        - ValueError: If the input dictionary is empty.
        - TypeError: If the input data is not a dictionary.
        - Exception: If an error occurs during the writing process.

        Returns:
        - None
        """
        # Check if data_dict is a dictionary
        if not isinstance(data_dict, dict):
            raise TypeError("Input data is not a dictionary.")

        # Check if the dictionary is empty
        if not data_dict:
            raise ValueError("Input dictionary is empty.")

        try:
            # Convert the dictionary to a DataFrame
            df = pd.DataFrame(list(data_dict.items()))

            # Write the DataFrame to a CSV file
            df.to_csv(csv_file, index=False)
            logger.info(f"Dictionary written to CSV: {csv_file}")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Error during CSV writing: {e}")
        except PermissionError as e:
            raise PermissionError(f"Error during CSV writing: {e}")
        except ValueError as e:
            raise ValueError(f"Error during CSV writing: {e}")
        except TypeError as e:
            raise TypeError(f"Error during CSV writing: {e}")

    @staticmethod
    def list_to_json(data_list, json_file):
        """
        Write a list to a JSON file.

        Parameters:
        - data_list (list): The input list to be written to the JSON file.
        - json_file (str): Path to the output JSON file.

        Raises:
        - TypeError: If the input data is not a list.
        - ValueError: If the input list is empty.
        - Exception: If an error occurs during the writing process.

        Returns:
        - None
        """
        # Check if data_list is a list
        if not isinstance(data_list, list):
            raise TypeError("Input data is not a list.")

        # Check if the list is empty
        if not data_list:
            raise ValueError("Input list is empty.")

        try:
            # Write the list to a JSON file
            with open(json_file, "w") as file:
                json.dump(data_list, file, indent=2)
            logger.info(f"List written to JSON: {json_file}")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Error during JSON writing: {e}")

    @staticmethod
    def create_folders(folder_paths):
        """
        Create folders (directories).

        Parameters:
        - folder_paths (list): List of folder paths to be created.

        Raises:
        - TypeError: If the input data is not a list.
        - ValueError: If the input list is empty.
        - OSError: If a folder cannot be created.

        Returns:
        - None
        """
        # Check if folder_paths is a list
        if not isinstance(folder_paths, list):
            raise TypeError("Input data is not a list.")

        # Check if the list of folder paths is empty
        if not folder_paths:
            raise ValueError("Input list of folder paths is empty.")

        try:
            # Create folders
            for folder_path in folder_paths:
                os.makedirs(folder_path, exist_ok=True)
                logger.info(f"Folder created: {folder_path}")
        except OSError as e:
            raise OSError(f"Error creating folder: {e}")

    @staticmethod
    def read_parquet(parquet_file, show_sample=False):
        """
        Read data from a Parquet file into a pandas DataFrame.

        Parameters:
        - parquet_file (str): Path to the input Parquet file.

        Raises:
        - FileNotFoundError: If the specified Parquet file is not found.
        - Exception: If an error occurs during the reading process.

        Returns:
        - pd.DataFrame: The data read from the Parquet file.
        """
        if not parquet_file:
            raise ValueError("Parquet file path is empty.")

        try:
            # Read Parquet file into a pandas DataFrame
            df = pd.read_parquet(parquet_file)
            logger.info(f"Parquet file read successfully: {parquet_file}")
            print(df) if show_sample else None
            return df
        except FileNotFoundError as fnfe:
            raise FileNotFoundError(f"Parquet file not found: {fnfe}")
        except Exception as e:
            raise Exception(f"Error during Parquet reading: {e}")

    @staticmethod
    def compact_parquet_files(input_dir, output_file):
        """
        Compact Parquet files in a directory into a single Parquet file.

        Parameters:
        - input_dir (str): The path to the directory containing Parquet files.
        - output_file (str): The path to the output Parquet file.

        Returns:
        None
        """
        # Check if the input directory exists
        if not os.path.exists(input_dir):
            raise FileNotFoundError(
                f"Input directory '{input_dir}' not found.",
            )

        if os.path.exists(output_file):  # Check if the output file already exists
            os.remove(output_file)

        # Get a list of all Parquet files in the input directory
        parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))

        # Check if there are any Parquet files in the input directory
        if not parquet_files:
            raise ValueError(
                f"No Parquet files found in the input directory '{input_dir}'.",
            )

        # Read each Parquet file into a DataFrame and concatenate them
        dfs = [pd.read_parquet(file) for file in parquet_files]
        concatenated_df = pd.concat(dfs, ignore_index=True)

        # Write the concatenated DataFrame to a new Parquet file
        concatenated_df.to_parquet(output_file, index=False)

    @staticmethod
    def check_empty_list(data_list):
        """
        Check if a list is empty.

        Parameters:
        - data_list (list): The list to be checked.

        Raises:
        - TypeError: If the input data is not a list.
        - ValueError: If the input list is empty.

        Returns:
        - bool: True if the list is empty, False otherwise.
        """
        # Check if data_list is a list
        if not isinstance(data_list, list):
            logger.warning(f"Input data is not a list: {data_list}")
            return False

        # Check if the list is empty
        if not data_list:
            logger.warning(f"Input list is empty: {data_list}")
            return False

        return True

    @staticmethod
    def delete_files(directory, file_filter):
        """
        Delete files in a directory based on a filter.

        Parameters:
        - directory (str): The directory path where files are located.
        - file_filter (str): The filter to match files for deletion.

        Returns:
        - int: The number of deleted files.
        """
        # Construct the file path pattern based on the directory and filter
        file_pattern = os.path.join(directory, file_filter)

        # Use glob to find files that match the pattern
        matching_files = glob.glob(file_pattern)

        # Delete each matching file
        for file_path in matching_files:
            os.remove(file_path)

        # Return the number of deleted files
        return len(matching_files)

    @staticmethod
    def delete_file(file_path):
        """
        Delete a file.

        Args:
            file_path (str): The path to the file to be deleted.

        Raises:
            FileNotFoundError: If the specified file is not found.
            PermissionError: If there are permission issues while attempting to delete the file.
            Exception: If any other unexpected error occurs during the file deletion process.

        Returns:
            None
        """
        try:
            os.remove(file_path)
            logger.info(f"File '{file_path}' deleted successfully.")
        except FileNotFoundError:
            logger.error(f"File '{file_path}' not found.")
        except PermissionError:
            logger.error(f"Permission denied. Unable to delete '{file_path}'.")
        except Exception as e:
            logger.error(f"An error occurred while deleting '{file_path}': {str(e)}")

    @staticmethod
    def remove_directory(directory_path):
        """
        Remove a directory and its contents.

        Parameters:
        - directory_path (str): The path to the directory to be removed.

        Raises:
        - FileNotFoundError: If the specified directory does not exist.
        - PermissionError: If the user does not have permission to remove the directory.

        Returns:
        - None
        """
        try:
            # Check if the directory exists
            if not os.path.exists(directory_path):
                raise FileNotFoundError(
                    f"The directory '{directory_path}' does not exist.",
                )

            # Remove the directory and its contents
            for root, dirs, files in os.walk(directory_path, topdown=False):
                for file in files:
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    os.rmdir(dir_path)

            # Remove the top-level directory
            os.rmdir(directory_path)

        except PermissionError as e:
            raise PermissionError(f"Permission error: {e}")

    @staticmethod
    def count_files(directory, extension=None):
        """
        Count the total number of files recursively in a directory.

        Parameters:
        - directory (str): The directory path.
        - extension (str or None): The file extension to filter by. If None, count all files.

        Returns:
        - int: The total number of files.
        """
        total_files = 0

        for root, dirs, files in os.walk(directory):
            for file in files:
                if extension is None or file.endswith(f".{extension}"):
                    total_files += 1

        return total_files

    @staticmethod
    def list_files_recursive(directory, extension=None, level=None):
        """
        Recursively list files in a directory with an optional extension filter.

        Parameters:
        - directory (str): The path to the directory to start listing files from.
        - extension (str, optional): If provided, only files with this extension will be included.
        - level (int, optional): If provided, limits the recursion depth.

        Returns:
        - list: A list of file paths.

        Example:
        ```python
        files = list_files_recursive('/path/to/directory', extension='txt', level=2)
        ```

        Note:
        - This function does not include directories in the result.
        """
        file_list = []

        for root, dirs, files in os.walk(directory):
            current_level = root.count(os.sep)
            if level is not None and current_level > level:
                continue

            for file in files:
                if extension is None or file.endswith(f".{extension}"):
                    file_list.append(os.path.join(root, file))

        return file_list

    @staticmethod
    def list_directories_recursive(directory, level=None):
        """
        Recursively list directories in a directory with an optional depth limit.

        Parameters:
        - directory (str): The path to the directory to start listing directories from.
        - level (int, optional): If provided, limits the recursion depth.

        Returns:
        - list: A list of directory paths.

        Example:
        ```python
        directories = list_directories_recursive('/path/to/directory', level=2)
        ```

        Note:
        - This function does not include the root directory in the result.
        """
        directory_list = []
        from pathlib import Path

        for root, dirs, files in os.walk(directory):
            current_level = str(Path(root)).count(os.sep)
            print(root, current_level)
            if level is not None and current_level > level:
                continue
            else:
                directory_list.append(os.path.basename(root))

        return directory_list

    @staticmethod
    def generate_random_directory(base_path, num_directories):
        """
        Generate random directories.

        Parameters:
        - base_path (str): The base path where directories will be created.
        - num_directories (int): The number of random directories to generate.

        Returns:
        - List[str]: List of the created directory paths.
        """
        created_directories = []

        for _ in range(num_directories):
            # Generate a random directory name
            random_name = "".join(random.choices(string.ascii_letters + string.digits, k=8))

            # Create the full path for the new directory
            new_directory_path = os.path.join(base_path, random_name)

            # Create the directory
            os.makedirs(new_directory_path)

            # Append the path to the list of created directories
            created_directories.append(new_directory_path)

        return created_directories

    @staticmethod
    def convert_date_to_epoch(date_string):
        date_format = "%Y-%m-%dT%H:%M:%S.%fGMT"

        # Convert the string to a datetime object
        dt_object = datetime.strptime(date_string, date_format)

        # Convert the datetime object to epoch time
        epoch_time = int(dt_object.replace(tzinfo=timezone.utc).timestamp())

        return epoch_time

    @staticmethod
    def move_directory(source_directory, destination_directory):
        """
        Move a directory to another directory.

        Args:
            source_directory (str): The path to the source directory to be moved.
            destination_directory (str): The path to the destination directory.

        Raises:
            FileNotFoundError: If the source directory is not found.
            PermissionError: If there are permission issues while attempting to move the directory.
            shutil.Error: If any other error occurs during the directory move process.

        Returns:
            None
        """
        try:
            shutil.move(source_directory, destination_directory)
            logger.info(f"Directory '{source_directory}' moved to '{destination_directory}' successfully.")
        except FileNotFoundError:
            logger.error(f"Source directory '{source_directory}' not found.")
        except PermissionError:
            logger.error(f"Permission denied. Unable to move '{source_directory}'.")
        except shutil.Error as e:
            logger.error(f"An error occurred while moving '{source_directory}': {str(e)}")

    @staticmethod
    def rename_file(old_file_path, new_file_name):
        """
        Rename a file.

        Args:
            old_file_path (str): The path to the file to be renamed.
            new_file_name (str): The new name for the file.

        Raises:
            FileNotFoundError: If the specified file is not found.
            PermissionError: If there are permission issues while attempting to rename the file.
            Exception: If any other unexpected error occurs during the file renaming process.

        Returns:
            None
        """
        try:
            # Extract the directory path from the old file path
            directory_path, old_file_name = os.path.split(old_file_path)

            # Construct the new file path by joining the directory path and the new file name
            new_file_path = os.path.join(directory_path, new_file_name)

            # Rename the file
            os.rename(old_file_path, new_file_path)

            logger.info(f"File '{old_file_path}' renamed to '{new_file_path}' successfully.")
        except FileNotFoundError:
            logger.error(f"File '{old_file_path}' not found.")
        except PermissionError:
            logger.error(f"Permission denied. Unable to rename '{old_file_path}'.")
        except Exception as e:
            logger.error(f"An error occurred while renaming '{old_file_path}': {str(e)}")

    @staticmethod
    def generate_random_string(length):
        """
        Generate a random string of the specified length.

        Parameters:
        - length (int): The desired length of the random string.

        Returns:
        - str: A random string consisting of uppercase letters, lowercase letters, and digits.
        """
        characters = string.ascii_letters + string.digits
        random_string = "".join(random.choice(characters) for _ in range(length))
        return random_string
