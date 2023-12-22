import glob
import json
import os

import pandas as pd

from spark_tunning_ml.config import config
from spark_tunning_ml.data import Data as data
from spark_tunning_ml.embeddings import Embeddings
from spark_tunning_ml.logger import logger
from spark_tunning_ml.milvus import MilvusWrapper

TRANSFORMATIONS = [
    "map",
    "filter",
    "flatMap",
    "mapPartitions",
    "mapPartitionsWithIndex",
    "sample",
    "union",
    "intersection",
    "distinct",
    "groupByKey",
    "reduceByKey",
    "aggregateByKey",
    "sortByKey",
    "join",
    "cogroup",
    "cartesian",
    "pipe",
    "coalesce",
    "repartition",
    "repartitionAndSortWithinPartitions",
]

ACTIONS = [
    "reduce",
    "collect",
    "count",
    "first",
    "take",
    "takeSample",
    "takeOrdered",
    "saveAsTextFile",
    "saveAsSequenceFile",
    "saveAsObjectFile",
    "countByKey",
    "foreach",
]


class Vectors:
    def __init__(self):
        """
        Initializes a new instance of the class.

        Parameters:
            self: The object itself.

        Returns:
            None.
        """
        self.milvus = MilvusWrapper()

    def build_vector(
        self,
        list_apps=[],
        data_source_path="data/applications",
        path_vector="/tmp/vectors",
    ):
        """
        Generates a vector by processing data from the specified data source path.

        Args:
            data_source_path (str, optional): The path to the data source. Defaults to "data/applications".

        Returns:
            str: The path to the generated vector.
        """

        # Get the path to the stage info from the configuration
        stage_path = config.get("spark_ui_path_stage_info")
        environment_path = config.get("spark_ui_path_environment")

        # Loop through each application
        for app in list_apps:
            # Initialize empty dataframes for stage and tasks
            df_stage = pd.DataFrame()
            df_tasks = pd.DataFrame()

            try:
                # Read the JSON file
                json_environment = data.list_files_recursive(
                    directory=f"{data_source_path}/{app}/{environment_path}",
                    extension="json",
                )

                if len(json_environment) == 0:
                    logger.error("Environment file not found")
                    break

                with open(json_environment[0], "r") as data_environment_file:
                    json_environment_data = data_environment_file.read()

                # Parse the JSON content
                json_environment = json.loads(json_environment_data)

                if "spark.app.id" in json_environment[0] and "spark.app.name" in json_environment[0]:
                    spark_app_id = json_environment[0].get("spark.app.id")
                    spark_app_name = json_environment[0].get("spark.app.name")

                    logger.info(f"Processing {spark_app_id} - {spark_app_name}")
                else:
                    logger.error("id or name not found in environment")
                    break

            except Exception as e:
                # Log any errors encountered while processing the file
                logger.error(f"Error processing environment file {json_environment}: {str(e)}")

            # Get a list of all JSON files in the application's stage path
            list_stages_json = data.list_files_recursive(
                directory=f"{data_source_path}/{app}/{stage_path}", extension="json"
            )

            # Loop through each JSON file
            for count, json_stage in enumerate(list_stages_json):
                try:
                    # Read the JSON file
                    with open(json_stage, "r") as data_stage_file:
                        json_data = data_stage_file.read()

                    # Parse the JSON content
                    json_stage_content = json.loads(json_data)

                    if "stageId" not in json_stage_content[0]:
                        break

                    stage_id = [json_stage_content[0].get("stageId")]

                    # Extract the stage data
                    data_stage = {
                        key: [json_stage_content[0].get(key)] for key in config.get("internal_vector_stage_columns")
                    }

                    data_stage["sparkAppId"] = [spark_app_id]
                    data_stage["sparkAppName"] = [spark_app_name]

                    # Extract and normalize the task data
                    for key, value in json_stage_content[0].get("tasks", {}).items():
                        json_normalized = pd.json_normalize(value).assign(stageId=stage_id)
                        df_tasks = pd.concat([df_tasks, json_normalized])

                    # Append the stage data to the stage dataframe
                    df_stage = pd.concat([df_stage, pd.DataFrame(data_stage)])

                except Exception as e:
                    # Log any errors encountered while processing the file
                    logger.error(f"Error processing file {json_stage}: {str(e)}")

            try:
                # Define the aggregation dictionary for tasks
                agg_dict = {
                    key: config.get("internal_vector_tasks_aggegation_metrics")
                    for key in config.get("internal_vector_tasks_columns")
                }

                # Group and aggregate the task data
                df_tasks_agg = df_tasks.groupby(["stageId"]).agg(agg_dict)
                df_tasks_agg.columns = [f"{col[0]}_{col[1]}Agg" for col in df_tasks_agg.columns]

                # Combine the stage and task data based on stageId
                df_combined = pd.merge(
                    df_stage,
                    df_tasks_agg,
                    on="stageId",
                    how="inner",
                    validate="one_to_one",
                )

                df_combined = df_combined.reset_index()

                # Define the output file path
                path_vector_path_app = f"{path_vector}/{app}.csv"

                # Save the combined data to a CSV file
                df_combined.to_csv(path_vector_path_app, index=False)

                # Log the successful processing and saving of data
                logger.info(f"Processed and saved data for {app} to {path_vector_path_app}")
            except Exception as e:
                logger.error(f"Error processing aggregation file {json_stage}: {str(e)}")
                continue

        # Return the path to the generated vector
        return path_vector

    def milvus_load_data(self, path_files):
        spark_collection = config.get("internal_milvus_collection_spark_metrics")
        spark_fields = config.get("internal_milvus_fields_spark_metrics")

        OUTPUT_FILE = "merge_all_applications.csv"

        self.milvus.connect()

        logger.info(self.milvus.list_collections())

        if self.milvus.has_collection(spark_collection):
            self.milvus.drop_collection(spark_collection)

        self.milvus.create_collection(spark_collection, spark_fields)

        all_files = glob.glob(os.path.join(path_files, "*.csv"))

        df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
        df.to_csv(f"{path_files}/{OUTPUT_FILE}", index=False)

        embedding_instance = Embeddings(model_name=config.get("internal_milvus_model_embeddins"))

        exclude_own_langchain = config.get("internal_milvus_fields_exclude_own_langchain")
        list_field_schema = [item for item in spark_fields if item not in exclude_own_langchain]

        data_vector = embedding_instance.build_entities(f"{path_files}/{OUTPUT_FILE}", list_field_schema)
        self.milvus.insert_data(data_vector)

        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 1024},
        }
        self.milvus.create_index(field_name="vector", index_params=index_params)

        logger.info(f"Number of entities in {spark_collection}: {self.milvus.get_entity_num()}")

        logger.info("Loading collection...")
        self.milvus.load_collection()

        logger.info("End process")

    def test_milvus(self):
        self.milvus.connect()

        spark_collection = config.get("internal_milvus_collection_spark_metrics")

        self.milvus.get_collection(spark_collection)

        logger.info(f"Loading collection {spark_collection}")

        self.milvus.load_collection()

        logger.info(f"Number of entities in collection: {self.milvus.get_entity_num()}")

        logger.info("End process")
