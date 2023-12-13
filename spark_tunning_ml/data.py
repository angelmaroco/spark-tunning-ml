from __future__ import annotations

import glob
import json
import os
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
            raise TypeError('Input data is not a dictionary.')

        # Check if the dictionary is empty
        if not data_dict:
            raise ValueError('Input dictionary is empty.')

        try:
            # Convert the dictionary to a DataFrame
            df = pd.DataFrame(list(data_dict.items()))

            # Write the DataFrame to a CSV file
            df.to_csv(csv_file, index=False)
            logger.info(f'Dictionary written to CSV: {csv_file}')
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Error during CSV writing: {e}')
        except PermissionError as e:
            raise PermissionError(f'Error during CSV writing: {e}')
        except ValueError as e:
            raise ValueError(f'Error during CSV writing: {e}')
        except TypeError as e:
            raise TypeError(f'Error during CSV writing: {e}')

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
            raise TypeError('Input data is not a list.')

        # Check if the list is empty
        if not data_list:
            raise ValueError('Input list is empty.')

        try:
            # Write the list to a JSON file
            with open(json_file, 'w') as file:
                json.dump(data_list, file, indent=2)
            logger.info(f'List written to JSON: {json_file}')
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Error during JSON writing: {e}')

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
            raise TypeError('Input data is not a list.')

        # Check if the list of folder paths is empty
        if not folder_paths:
            raise ValueError('Input list of folder paths is empty.')

        try:
            # Create folders
            for folder_path in folder_paths:
                os.makedirs(folder_path, exist_ok=True)
                logger.info(f'Folder created: {folder_path}')
        except OSError as e:
            raise OSError(f'Error creating folder: {e}')

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
            raise ValueError('Parquet file path is empty.')

        try:
            # Read Parquet file into a pandas DataFrame
            df = pd.read_parquet(parquet_file)
            logger.info(f'Parquet file read successfully: {parquet_file}')
            print(df) if show_sample else None
            return df
        except FileNotFoundError as fnfe:
            raise FileNotFoundError(f'Parquet file not found: {fnfe}')
        except Exception as e:
            raise Exception(f'Error during Parquet reading: {e}')

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
        parquet_files = glob.glob(os.path.join(input_dir, '*.parquet'))

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
            logger.error(f'Input data is not a list: {data_list}')
            return False

        # Check if the list is empty
        if not data_list:
            logger.error(f'Input list is empty: {data_list}')
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
            raise PermissionError(f'Permission error: {e}')
