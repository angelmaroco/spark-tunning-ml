from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor

from spark_tunning_ml.config import config
from spark_tunning_ml.data import Data as data
from spark_tunning_ml.logger import logger
from spark_tunning_ml.spark_ui_wrapper import SparkUIWrapper


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
        f'Processing executors for application {id} and attempt {attemptid}',
    )

    id_uri = f'{id}/{attemptid}' if attemptid > 0 else id

    executors = sparkui.get_executors(id_uri)

    if data.check_empty_list(executors):
        data.list_to_json(executors, f'{path_executors}/raw.json')
        data.json_to_parquet(
            executors,
            f'{path_executors}/data.parquet',
            add_column_value={'application_id': id},
        )
        return True
    else:
        logger.info('No executors found.')
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
        f'Processing stages for application {id} and attempt {attemptid}',
    )
    id_uri = f'{id}/{attemptid}' if attemptid > 0 else id

    stages = sparkui.get_stages(id_uri)

    if data.check_empty_list(stages):
        data.list_to_json(stages, f'{path_stages}/raw.json')
        data.json_to_parquet(
            data_list=stages,
            parquet_file=f'{path_stages}/data.parquet',
            add_column_value={'application_id': id},
        )
        return stages
    else:
        logger.info('No stages found.')
        return []


def process_stage_task_detail(sparkui, path_stage_tasks_detail, id, attemptid, stage, stage_attempt_id):
    """
    Process the task detail for a specific stage.

    Args:
        sparkui (SparkUI): An instance of the SparkUI class.
        path_stage_tasks_detail (str): The path to the directory where the task detail files will be saved.
        id (str): The ID of the application.
        attemptid (int): The attempt ID of the application.
        stage (int): The ID of the stage.
        stage_attempt_id (int): The ID of the stage attempt.

    Returns:
        bool: True if the task detail is successfully processed, False otherwise.
    """
    logger.info(
        f'Processing stage task detail for application {id} and attempt {attemptid}',
    )
    id_uri = f'{id}/{attemptid}' if attemptid > 0 else id

    response_stage_tasks_detail = sparkui.get_task_list(
        id_uri,
        stage,
        stage_attempt_id,
    )

    if data.check_empty_list(response_stage_tasks_detail):
        data.list_to_json(
            response_stage_tasks_detail,
            f'{path_stage_tasks_detail}/raw-{stage}-{stage_attempt_id}-task.json',
        )
        data.json_to_parquet(
            data_list=response_stage_tasks_detail,
            parquet_file=f'{path_stage_tasks_detail}/data-{stage}-{stage_attempt_id}-task.parquet',
            add_column_value={
                'application_id': id,
                'stage_id': stage,
                'stage_attempt_id': stage_attempt_id,
            },
        )
        return True
    else:
        logger.info('No tasks detail found.')
        return False


def process_stage_task_summary(sparkui, path_stage_tasks_summary, id, attemptid, stage, stage_attempt_id):
    """
    Process the task summary for a specific stage of a Spark application.
    Args:
        sparkui (SparkUI): The Spark UI object.
        path_stage_tasks_summary (str): The path to save the stage tasks summary.
        id (str): The application ID.
        attemptid (int): The attempt ID of the application.
        stage (int): The stage number.
        stage_attempt_id (int): The attempt ID of the stage.
    Returns:
        bool: True if the task summary processing is successful, False otherwise.
    """
    logger.info(
        f'Processing stage task summary for application {id} and attempt {attemptid}',
    )
    id_uri = f'{id}/{attemptid}' if attemptid > 0 else id

    response_stage_tasks_summary = [
        sparkui.get_task_summary(id_uri, stage, stage_attempt_id),
    ]

    if data.check_empty_list(response_stage_tasks_summary):
        data.list_to_json(
            response_stage_tasks_summary,
            f'{path_stage_tasks_summary}/raw-{stage}-{stage_attempt_id}-task.json',
        )
        data.json_to_parquet(
            data_list=response_stage_tasks_summary,
            parquet_file=f'{path_stage_tasks_summary}/data-{stage}-{stage_attempt_id}-task.parquet',
            add_column_value={
                'application_id': id,
                'stage_id': stage,
                'stage_attempt_id': stage_attempt_id,
            },
        )
        return True
    else:
        logger.info('No tasks summary found.')
        return False


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
        f'Processing stage for application {id} and attempt {attemptid}',
    )
    id_uri = f'{id}/{attemptid}' if attemptid > 0 else id

    stages = sparkui.get_ids_form_stages(stages)
    list_raw_stages = []

    if data.check_empty_list(stages):
        for stage in stages:
            logger.info(f'Processing stage {stage} for application {id}')
            stage_response = sparkui.get_stage(id_uri, stage)

            data.list_to_json(stage_response, f'{path_stage}/raw-{stage}.json')
            data.json_to_parquet(
                data_list=stage_response,
                parquet_file=f'{path_stage}/stage-{stage}.parquet',
                add_column_value={'application_id': id},
                select_columns=config.get('spark_ui_api_stage_columns'),
            )
            list_raw_stages.append(stage_response[0].get('tasks'))

        return list_raw_stages
    else:
        logger.info('No stages found.')
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
        f'Processing jobs for application {id} and attempt {attemptid}',
    )
    id_uri = f'{id}/{attemptid}' if attemptid > 0 else id

    jobs = sparkui.get_jobs(id_uri)

    if data.check_empty_list(jobs):
        data.list_to_json(jobs, f'{path_jobs}/raw.json')
        data.json_to_parquet(
            jobs,
            f'{path_jobs}/data.parquet',
            add_column_value={'application_id': id},
        )
        return True
    else:
        logger.info('No jobs found.')
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
        f'Processing environment for application {id} and attempt {attemptid}',
    )
    id_uri = f'{id}/{attemptid}' if attemptid > 0 else id

    environment = [sparkui.get_environment(id_uri)]

    spark_properties = [sparkui.get_environment_spark_properties(environment)]

    if data.check_empty_list(spark_properties):
        data.list_to_json(spark_properties, f'{path_environment}/raw.json')
        data.json_to_parquet(
            spark_properties,
            f'{path_environment}/data.parquet',
            add_column_value={'application_id': id},
        )
        return True
    else:
        logger.info('No environment found.')
        return False


def process_tasks_stage(sparkui, raw_stages, path_stage_tasks_detail, id, attemptid):
    data_raw = sparkui.get_tasks_from_stages_normalized(raw_stages)

    logger.info(
        f'Processing tasks for application {id} and attempt {attemptid}',
    )

    if data.check_empty_list(data_raw):
        data.list_to_json(data_raw, f'{path_stage_tasks_detail}/raw.json')
        data.json_to_parquet(
            data_raw,
            f'{path_stage_tasks_detail}/data.parquet',
            add_column_value={'application_id': id},
        )
        return True
    else:
        logger.info('No tasks found.')
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
    logger.info(f'Processing application {id}')

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
        logger.info('No stages found. Ommiting application')
        data.remove_directory(path_root)
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

        # process_executors(sparkui, paths[0], id, attemptid)

        # process_jobs(sparkui, paths[4], id, attemptid)

        # process_environment(sparkui, paths[5], id, attemptid)


def main():
    """
    Entry point of the program.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Command-line usage of SparkTunningML.',
    )
    parser.add_argument(
        '--sparkui_api_url',
        required=True,
        help='The base URL of the Spark UI API (e.g. http://spark-ui-api:8080/api/v1).',
    )
    args = parser.parse_args()

    # Initialize SparkUIWrapper with the base URL
    sparkui = SparkUIWrapper(args.sparkui_api_url)

    # Get the version of Spark API
    version = sparkui.get_version().get('spark', 'unknown')

    # Check if the version is allowed
    if version not in config.get('internal_spark_ui_compatible_versions'):
        logger.critical(f'Spark API version {version} not allowed.')
        sys.exit()
    else:
        logger.info(f'Spark API version: {version}')

    apps_limit = config.get("internal_spark_ui_apps_limit")
    # Get the list of applications from Spark UI
    applications = sparkui.get_applications(apps_limit)

    # Get the application IDs from the applications list
    applications_ids = sparkui.get_ids_from_applications(applications)

    # Enable debug mode if configured
    debug_mode_enabled = config.get('internal_spark_ui_debug_mode_enabled')
    max_apps = config.get('internal_spark_ui_debug_mode_max_apps')
    test_concurrency = config.get(
        'internal_spark_ui_debug_mode_test_concurrency')

    if debug_mode_enabled:
        logger.info(f'Debug mode enabled. Max applications: {max_apps}')
        applications_ids = applications_ids[:max_apps]

        if test_concurrency:
            logger.info('Test concurrency enabled')
            applications_ids *= config.get(
                'internal_spark_ui_debug_mode_test_concurrency_apps',
            )

    # Check if the applications list is empty
    if not data.check_empty_list(applications_ids):
        sys.exit()

    # Create a ThreadPoolExecutor object with the maximum concurrency limit specified in the configuration
    # This will allow us to execute multiple tasks concurrently
    with ThreadPoolExecutor(config.get('internal_spark_ui_max_concurrency')) as executor:
        for app in applications_ids:
            for id, attempid in app.items():
                executor.submit(process_application, id, attempid, sparkui)


if __name__ == '__main__':
    main()
