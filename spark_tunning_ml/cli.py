from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from spark_tunning_ml.audit import audit
from spark_tunning_ml.azure import AzureBlobStorageHandler
from spark_tunning_ml.config import config
from spark_tunning_ml.data import Data as data
from spark_tunning_ml.logger import logger
from spark_tunning_ml.spark_ui_handler import SparkUIHandler
from spark_tunning_ml.vectors import Vectors


def process_executors(sparkui, path_executors, id, attemptid):
    """
    Process the executors for a given Spark application.

    Args:
        sparkui (SparkUI): The SparkUI object.
        path_executors (str): The path to the executors data.
        id (int): The application ID.
        attemptid (int): The attempt ID of the application.

    Returns:
        bool: True if the executors were processed successfully, False otherwise.
    """
    logger.info(
        f"Processing executors for application {id} and attempt {attemptid}",
    )

    id_uri = f"{id}/{attemptid}" if attemptid > 0 else id

    executors = sparkui.get_executors(id_uri)

    if data.check_empty_list(executors):
        data.list_to_json(executors, f"{path_executors}/raw.json")
        return True
    else:
        logger.info("No executors found.")
        return False


def process_stages(sparkui, path_stages, id, attemptid):
    """
    Process stages for an application.

    Args:
        sparkui (SparkUI): The SparkUI object.
        path_stages (str): The path to the stages.
        id (int): The application ID.
        attemptid (int): The attempt ID of the application.

    Returns:
        list: The list of stages if not empty, otherwise an empty list.
    """
    logger.info(
        f"Processing stages for application {id} and attempt {attemptid}",
    )
    id_uri = f"{id}/{attemptid}" if attemptid > 0 else id

    stages = sparkui.get_stages(id_uri)

    if data.check_empty_list(stages):
        data.list_to_json(stages, f"{path_stages}/raw.json")
        return stages
    else:
        logger.info("No stages found.")
        return []


def process_stage(sparkui, path_stage, stages, id, attemptid):
    """
    Process each stage in the given list of stages for the specified application ID.

    Args:
        sparkui: An instance of the SparkUI class.
        path_stage: Path to the directory where the stage data will be stored.
        stages: List of stage IDs to process.
        id: Application ID.
        attemptid (int): The attempt ID of the application.

    Returns:
        A dictionary mapping each stage ID to its corresponding attempt ID.
    """
    logger.info(
        f"Processing stage for application {id} and attempt {attemptid}",
    )
    id_uri = f"{id}/{attemptid}" if attemptid > 0 else id

    stages = sparkui.get_ids_form_stages(stages)
    list_raw_stages = []

    if data.check_empty_list(stages):
        for stage in stages:
            logger.info(f"Processing stage {stage} for application {id}")
            stage_response = sparkui.get_stage(id_uri, stage)

            data.list_to_json(stage_response, f"{path_stage}/raw-{stage}.json")
            list_raw_stages.append(stage_response[0].get("tasks"))

        return list_raw_stages
    else:
        logger.info("No stage found.")
        return []


def process_jobs(sparkui, path_jobs, id, attemptid):
    """
    Process jobs for a given application ID.

    Args:
        sparkui: The SparkUI instance.
        path_jobs: The path to the jobs directory.
        id: The application ID.
        attemptid (int): The attempt ID of the application.

    Returns:
        bool: True if jobs were processed successfully, False otherwise.
    """
    logger.info(
        f"Processing jobs for application {id} and attempt {attemptid}",
    )
    id_uri = f"{id}/{attemptid}" if attemptid > 0 else id

    jobs = sparkui.get_jobs(id_uri)

    if data.check_empty_list(jobs):
        data.list_to_json(jobs, f"{path_jobs}/raw.json")
        return True
    else:
        logger.info("No jobs found.")
        return False


def process_environment(sparkui, path_environment, id, attemptid):
    """
    Process environment for the given application ID.

    Args:
        sparkui (SparkUI): The SparkUI object.
        path_environment (str): The path to the environment directory.
        id (str): The application ID.
        attemptid (int): The attempt ID of the application.

    Returns:
        bool: True if environment is processed successfully, False otherwise.
    """
    logger.info(
        f"Processing environment for application {id} and attempt {attemptid}",
    )
    id_uri = f"{id}/{attemptid}" if attemptid > 0 else id

    environment = [sparkui.get_environment(id_uri)]

    spark_properties = [sparkui.get_environment_spark_properties(environment)]

    if data.check_empty_list(spark_properties):
        data.list_to_json(spark_properties, f"{path_environment}/raw.json")
        return True
    else:
        logger.info("No environment found.")
        return False


def process_tasks_stage(sparkui, raw_stages, path_stage_tasks_detail, id, attemptid):
    """
    Process tasks stage.

    Args:
        sparkui (SparkUI): The SparkUI instance.
        raw_stages (List[Stage]): A list of raw stages.
        path_stage_tasks_detail (str): The path to stage tasks detail.
        id (str): The application ID.
        attemptid (str): The attempt ID.

    Returns:
        bool: True if tasks were processed and saved successfully, False otherwise.
    """
    data_raw = sparkui.get_tasks_from_stages_normalized(raw_stages)

    logger.info(
        f"Processing tasks for application {id} and attempt {attemptid}",
    )

    if data.check_empty_list(data_raw):
        data.list_to_json(data_raw, f"{path_stage_tasks_detail}/raw.json")
        return True
    else:
        logger.info("No tasks found.")
        return False


def process_application(id, attemptid, sparkui):
    """
    Process the application with the given ID.

    Args:
        id (str): The ID of the application.
        attemptid (int): The attempt ID of the application.
        sparkui (SparkUI): The SparkUI object for the application.

    Returns:
        None
    """
    logger.info(f"Processing application {id}")

    internal_milvus_force_reprocess = config.get("internal_milvus_force_reprocess")

    if internal_milvus_force_reprocess:
        audit.delete_app_id(id)

    if audit.query_app_id(id):
        logger.info(f"Application {id} already processed. Skipping")
        return

    path_errors = config.get("spark_ui_path_errors")
    path_root = f'{config.get("spark_ui_path_root")}/{id}'

    paths = [
        f'{path_root}/{config.get("spark_ui_path_executors")}',
        f'{path_root}/{config.get("spark_ui_path_stages")}',
        f'{path_root}/{config.get("spark_ui_path_stage_info")}',
        f'{path_root}/{config.get("spark_ui_path_stage_tasks_detail")}',
        f'{path_root}/{config.get("spark_ui_path_jobs")}',
        f'{path_root}/{config.get("spark_ui_path_environment")}',
    ]

    data.create_folders(paths)

    stages = process_stages(sparkui, paths[1], id, attemptid)

    if not data.check_empty_list(stages):
        logger.info("No stages found. Ommiting application (moving to errors directory)")
        data.move_directory(path_root, path_errors)
    else:
        raw_stages = process_stage(
            sparkui,
            paths[2],
            stages,
            id,
            attemptid,
        )

        # Process stages without api rest
        process_tasks_stage(sparkui, raw_stages, paths[3], id, attemptid)

        process_executors(sparkui, paths[0], id, attemptid)

        process_jobs(sparkui, paths[4], id, attemptid)

        process_environment(sparkui, paths[5], id, attemptid)

    count_files_executors = data.count_files(paths[0])
    count_files_stages = data.count_files(paths[1])
    count_files_tasks = data.count_files(paths[3])
    count_files_jobs = data.count_files(paths[4])
    count_files_environment = data.count_files(paths[5])

    audit.add_app_id(
        id,
        1,
        count_files_executors,
        count_files_stages,
        count_files_tasks,
        count_files_jobs,
        count_files_environment,
    )


def parse_arguments():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description="Command-line usage of SparkTunningML.")
    parser.add_argument(
        "--sparkui_api_url",
        required=True,
        help="The base URL of the Spark UI API (e.g. http://spark-ui-api:8080/api/v1).",
    )
    return parser.parse_args()


def initialize_spark_ui(sparkui_api_url):
    """
    Initialize SparkUIWrapper with the base URL.
    """
    sparkui = SparkUIHandler(sparkui_api_url)
    return sparkui


def check_spark_version(version):
    """
    Check if the given Spark version is allowed.

    Parameters:
        version (str): The Spark version to check.

    Returns:
        None
    """
    allowed_versions = config.get("internal_spark_ui_compatible_versions")
    if version not in allowed_versions:
        logger.critical(f"Spark API version {version} not allowed.")
        sys.exit()
    else:
        logger.info(f"Spark API version: {version}")


def process_applications(sparkui):
    """
    Process applications in the SparkUI.

    Args:
        sparkui (SparkUI): The SparkUI object.

    Returns:
        None

    Description:
        This function processes applications in the SparkUI. It retrieves the applications from the SparkUI object and applies various filters and limits based on the configuration settings. It then uses ThreadPoolExecutor to process each application concurrently.

        - sparkui (SparkUI): The SparkUI object representing the SparkUI.

    """
    if not config.get("internal_spark_ui_load_applications"):
        logger.info("Omitting applications processing")
        return

    apps_limit = config.get("internal_spark_ui_apps_limit")
    applications = sparkui.get_applications(apps_limit)
    logger.info(f"Found {len(applications)} applications")

    filter_users_pattern = config.get("spark_ui_api_endpoint_applications_filter_user")
    applications_ids = sparkui.get_ids_from_applications(applications, filter_users_pattern=filter_users_pattern)

    debug_mode_enabled = config.get("internal_spark_ui_debug_mode_enabled")
    max_apps = config.get("internal_spark_ui_debug_mode_max_apps")
    test_concurrency = config.get("internal_spark_ui_debug_mode_test_concurrency")

    if debug_mode_enabled:
        logger.info(f"Debug mode enabled. Max applications: {max_apps}")
        applications_ids = applications_ids[:max_apps]

        if test_concurrency:
            logger.info("Test concurrency enabled")
            applications_ids *= config.get("internal_spark_ui_debug_mode_test_concurrency_apps")

    if not data.check_empty_list(applications_ids):
        sys.exit()

    concurrency_limit = config.get("internal_spark_ui_max_concurrency_api")
    with ThreadPoolExecutor(concurrency_limit) as executor:
        futures_executor = [
            executor.submit(process_application, id, attempid, sparkui)
            for app in applications_ids
            for id, attempid in app.items()
        ]


def process_milvus_data():
    """
    Process Milvus data by generating a data source for milvus vectors and loading it into Milvus.

    This function performs the following steps:
    1. Checks if the configuration has the flag "internal_milvus_load_data" set to True. If not, it logs a message and returns.
    2. Logs a message indicating that it is generating a data source for milvus vectors.
    3. Initializes a Vectors object.
    4. Sets the data source path to "data/applications".
    5. Lists all directories recursively under the data source path, up to level 2.
    6. Loops through the list of applications and performs the following operations:
       a. Checks if the application ID exists in the audit table. If not, adds a dummy record to the audit table.
       b. Checks if the application has already been processed by querying the audit table. If so, logs a message and removes the application from the list.
    7. Checks if the list of applications is empty. If so, generates a random directory path for storing the milvus vectors.
    8. Sets the concurrency limit for building vectors using the configuration value "internal_spark_ui_max_concurrency_vector".
    9. Uses a ThreadPoolExecutor to asynchronously build vectors for each application in the list of applications.
       The build_vector method of the Vectors object is called with the application, data source path, and path vector as arguments.
    10. Updates the application ID in the audit table with the result of building the vector.
    11. Loads the milvus vectors from the path vector into Milvus using the milvus_load_data method of the Vectors object.
    """
    if not config.get("internal_milvus_load_data"):
        logger.info("Omitting milvus load data")
        return

    logger.info("Generating data source for milvus vectors")
    vectors = Vectors()
    data_source_path = "data/applications"
    list_apps = data.list_directories_recursive(directory=data_source_path, level=2)

    for app in list_apps[:]:
        # Allows reprocessing of applications not available in API.
        # We add a dummy record to the audit table
        if not audit.query_app_id(app, 1):
            audit.add_app_id(app, 1, -1, -1, -1, -1, -1)

        if audit.query_app_id_load_vector(app):
            logger.info(f"{app} already processed")
            list_apps.remove(app)

    if data.check_empty_list(list_apps):
        path_vector = data.generate_random_directory(config.get("internal_vector_output_path"), 1)[0]

        concurrency_limit = config.get("internal_spark_ui_max_concurrency_vector")
        with ThreadPoolExecutor(concurrency_limit) as vector:
            futures_vector = [
                vector.submit(vectors.build_vector, [app], data_source_path, path_vector) for app in list_apps
            ]

        for app in futures_vector:
            audit.update_app_id(app.result()[0], 1)

        vectors.milvus_load_data(path_vector)


def process_milvus_collection():
    """
    Process the Milvus collection.

    This function checks if the configuration parameter "internal_milvus_load_collection" is set.
    If it is not set, the function logs a message and returns.
    If it is set, the function logs a message and proceeds to load the Milvus collection by calling the "milvus_load_collection" method of the "Vectors" class.

    Parameters:
    None

    Returns:
    None
    """
    if not config.get("internal_milvus_load_collection"):
        logger.info("Omitting milvus load collection")
        return

    logger.info("Loading milvus collection")
    vectors = Vectors()
    vectors.milvus_load_collection()


def uploads_files_to_blob_storage():
    """
    Uploads files to Azure Blob Storage.

    This function uploads files to Azure Blob Storage if the `internal_azure_upload_enabled` configuration is set to `True`.

    Parameters:
        None

    Returns:
        None
    """
    if not config.get("internal_azure_upload_enabled"):
        logger.info("Azure Blob Storage upload not enabled")
        return

    process_name = config.get("internal_process_name")
    container_name = f"{process_name}-{config.get('internal_azure_container_name')}"
    max_workers = config.get("internal_azure_upload_max_workers")
    azure_blob_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    force_remove_object_container = config.get("internal_azure_upload_container_force_remove")
    upload_compress_enabled = config.get("internal_azure_upload_compress_enabled")
    sources = config.get("spark_ui_path_root")

    if not azure_blob_connection_string:
        logger.error("Azure Blob Storage connection string not found")
        return

    logger.info("Starting upload files to Azure Blob Storage")
    blob_storage_instance = AzureBlobStorageHandler(azure_blob_connection_string, container_name)
    blob_storage_instance.create_container()

    if force_remove_object_container:
        logger.info("Removing all files in container")
        blob_storage_instance.drop_container()
        logger.warning("This operation may take a long time and the status of the operation cannot be known. Aborting.")

    list_files_upload = []

    if upload_compress_enabled:
        list_files_upload = compress_files(sources)
    else:
        list_files_upload = data.list_files_recursive(sources, "json")

    for file in list_files_upload.copy():
        app = str(Path(file)).split(os.sep)[2].split(".")[0]  # Example 'application_1692342312988_81566'

        if not audit.query_app_id(app, 1):
            audit.add_app_id(app, 1, -2, -2, -2, -2, -2)

        if audit.query_app_id_upload(app, 1):
            list_files_upload.remove(file)

    logger.info(f"Total files to upload: {len(list_files_upload)}") if list_files_upload else logger.info(
        "No files to upload"
    )

    results = blob_storage_instance.upload_blobs(list_files_upload, max_workers=max_workers)

    [audit.update_app_id_upload(app, int(state is True)) for app, state in results]

    if upload_compress_enabled:
        [data.delete_file(file) for file in list_files_upload]

    logger.info("Upload files to Azure Blob Storage completed")


def download_files_from_blob_storage():
    if not config.get("internal_azure_download_enabled"):
        logger.info("Azure Blob Storage download not enabled")
        return

    process_name = config.get("internal_process_name")
    container_name = f"{process_name}-{config.get('internal_azure_container_name')}"
    max_workers = config.get("internal_azure_download_max_workers")
    azure_blob_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

    target = f"{config.get('spark_ui_path_root')}"

    if not azure_blob_connection_string:
        logger.error("Azure Blob Storage connection string not found")
        return

    logger.info("Starting download files from Azure Blob Storage")
    blob_storage_instance = AzureBlobStorageHandler(azure_blob_connection_string, container_name)

    file_filter = "zip" if config.get("internal_azure_download_compress_enabled") else ""

    blob_storage_instance.download_blobs_in_folder("", file_filter=file_filter, max_workers=max_workers)

    logger.info("Download files to Azure Blob Storage completed")

    data.decompress_and_delete_zip_parallel(target) if config.get("internal_azure_download_compress_enabled") else None


def compress_files(sources):
    logger.info("Compressing files")
    list_apps = data.list_directories_recursive(directory=sources, level=2)
    list_apps_zip = [
        (os.path.join(sources, app), os.path.join(sources, app))
        for app in list_apps
        if not audit.query_app_id_upload(app, 1)
    ]
    data.compress_folders_parallel(list_apps_zip)
    logger.info(f"Found {len(list_apps_zip)} files to compress")
    return data.list_files_recursive(sources, "zip")


def main():
    """
    Initializes the main function and executes the following steps:
    1. Parses the command line arguments using `parse_arguments()`.
    2. Initializes the Spark UI by calling `initialize_spark_ui()` with the `args.sparkui_api_url` parameter.
    3. Retrieves the Spark version using `sparkui.get_version().get("spark", "unknown")`.
    4. Checks if the Spark version is valid by calling `check_spark_version()` with the `version` parameter.
    5. Processes the applications by calling `process_applications(sparkui)`.
    6. Uploads files to blob storage by calling `uploads_files_to_blob_storage()`.
    7. Processes Milvus data by calling `process_milvus_data()`.
    8. Processes Milvus collection by calling `process_milvus_collection()`.
    """
    args = parse_arguments()

    sparkui = initialize_spark_ui(args.sparkui_api_url)

    version = sparkui.get_version().get("spark", "unknown")
    check_spark_version(version)

    process_applications(sparkui)
    uploads_files_to_blob_storage()

    download_files_from_blob_storage()
    process_milvus_data()
    process_milvus_collection()


if __name__ == "__main__":
    main()
