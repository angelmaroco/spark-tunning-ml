import glob
import json
import os
from time import time

import pandas as pd

from spark_tunning_ml.config import config
from spark_tunning_ml.data import Data as data
from spark_tunning_ml.embeddings import Embeddings
from spark_tunning_ml.logger import logger
from spark_tunning_ml.milvus import MilvusHandler


class Vectors:
    def __init__(self):
        """
        Initializes a new instance of the class.

        Parameters:
            self: The object itself.

        Returns:
            None.
        """
        self.milvus = MilvusHandler(
            uri=os.environ.get("MILVUS_URI"),
            token=os.environ.get("MILVUS_TOKEN"),
        )

    def check_property(self, data, property, default_value=None):
        if property in data:
            return data[property]
        else:
            return default_value

    def build_vector(self, list_apps=[], data_source_path="data/applications", path_vector="/tmp/vectors"):
        stage_path = config.get("spark_ui_path_stage_info")
        environment_path = config.get("spark_ui_path_environment")

        PROPERTY_DEFAULT_VALUE_STRING = "N/A"
        PROPERTY_DEFAULT_VALUE_INT = 0
        PROPERTY_DEFAULT_STRING_BYTES = "0b"

        init_time = time()

        result_apps = []

        try:
            for app in list_apps:
                df_stage = pd.DataFrame()
                df_tasks = pd.DataFrame()

                try:
                    result_apps.append(app)

                    json_environment = data.list_files_recursive(
                        os.path.join(data_source_path, app, environment_path), extension="json"
                    )
                    if not json_environment:
                        logger.error(f"Environment file not found for {app}")
                        continue

                    with open(json_environment[0], "r") as data_environment_file:
                        json_environment_data = data_environment_file.read()

                    json_environment = json.loads(json_environment_data)[0]

                    if "spark.app.id" not in json_environment or "spark.app.name" not in json_environment:
                        logger.error("id or name not found in environment")
                        continue

                    spark_properties = json_environment
                    system_properties = json_environment

                    spark_app_id = self.check_property(
                        spark_properties, "spark.app.id", default_value=PROPERTY_DEFAULT_VALUE_STRING
                    )
                    spark_app_name = self.check_property(
                        spark_properties, "spark.app.name", default_value=PROPERTY_DEFAULT_VALUE_STRING
                    )
                    spark_executor_instances = self.check_property(
                        spark_properties, "spark.executor.instances", default_value=PROPERTY_DEFAULT_VALUE_INT
                    )
                    spark_tags = self.check_property(
                        spark_properties, "spark.yarn.tags", default_value=PROPERTY_DEFAULT_VALUE_STRING
                    )
                    config_file = self.check_property(
                        spark_properties, "config.file", default_value=PROPERTY_DEFAULT_VALUE_STRING
                    )
                    spark_queue = self.check_property(
                        spark_properties, "spark.yarn.queue", default_value=PROPERTY_DEFAULT_VALUE_STRING
                    )
                    spark_scheduler_minregisteredresourcesratio = self.check_property(
                        spark_properties,
                        "spark.scheduler.minRegisteredResourcesRatio",
                        default_value=PROPERTY_DEFAULT_VALUE_INT,
                    )
                    spark_memory_offheap_size = data.convert_to_bytes(self.check_property(
                        spark_properties, "spark.memory.offHeap.size", default_value=PROPERTY_DEFAULT_STRING_BYTES
                    ))
                    spark_executor_cores = self.check_property(
                        spark_properties, "spark.executor.cores", default_value=PROPERTY_DEFAULT_VALUE_INT
                    )
                    spark_executor_memory = data.convert_to_bytes(self.check_property(
                        spark_properties, "spark.executor.memory", default_value=PROPERTY_DEFAULT_STRING_BYTES
                    ))
                    spark_driver_cores = self.check_property(
                        spark_properties, "spark.driver.cores", default_value=PROPERTY_DEFAULT_VALUE_INT
                    )
                    spark_driver_memory = data.convert_to_bytes(self.check_property(
                        spark_properties, "spark.driver.memory", default_value=PROPERTY_DEFAULT_STRING_BYTES
                    ))
                    spark_driver_maxresultsize = data.convert_to_bytes(self.check_property(
                        spark_properties, "spark.driver.maxResultSize", default_value=PROPERTY_DEFAULT_STRING_BYTES
                    ))
                    spark_default_parallelism = self.check_property(
                        spark_properties, "spark.default.parallelism", default_value=PROPERTY_DEFAULT_VALUE_INT
                    )
                    spark_sql_shuffle_partitions = self.check_property(
                        spark_properties, "spark.sql.shuffle.partitions", default_value=PROPERTY_DEFAULT_VALUE_INT
                    )
                    spark_shuffle_file_buffer = data.convert_to_bytes(self.check_property(
                        spark_properties, "spark.shuffle.file.buffer", default_value=PROPERTY_DEFAULT_STRING_BYTES
                    ))
                    spark_reducer_maxsizeinflight = data.convert_to_bytes(self.check_property(
                        spark_properties, "spark.reducer.maxsizeinflight", default_value=PROPERTY_DEFAULT_STRING_BYTES
                    ))
                    spark_sql_autobroadcastjoin = self.check_property(
                        spark_properties,
                        "spark.sql.autoBroadcastJoinThreshold",
                        default_value=PROPERTY_DEFAULT_VALUE_INT,
                    )

                    spark_dynamic_allocation = int(
                        self.check_property(spark_properties, "spark.dynamicAllocation.enabled", default_value="0")
                        == "true"
                    )
                    spark_dynamic_allocation_initial = self.check_property(
                        spark_properties,
                        "spark.dynamicAllocation.initialExecutors",
                        default_value=PROPERTY_DEFAULT_VALUE_INT,
                    )
                    spark_dynamic_allocation_min = self.check_property(
                        spark_properties,
                        "spark.dynamicAllocation.minExecutors",
                        default_value=PROPERTY_DEFAULT_VALUE_INT,
                    )
                    spark_dynamic_allocation_max = self.check_property(
                        spark_properties,
                        "spark.dynamicAllocation.maxExecutors",
                        default_value=PROPERTY_DEFAULT_VALUE_INT,
                    )
                    spark_dynamic_allocation_executors_idle_timeout = self.check_property(
                        spark_properties,
                        "spark.dynamicAllocation.executorIdleTimeout",
                        default_value=PROPERTY_DEFAULT_VALUE_INT,
                    )

                    spark_user_name = self.check_property(
                        system_properties, "user.name", default_value=PROPERTY_DEFAULT_VALUE_STRING
                    )

                    logger.info(f"Processing {spark_app_id} - {spark_app_name} - {spark_tags}")

                except Exception as e:
                    logger.error(f"Error processing environment file {json_environment}: {str(e)}")
                    continue

                list_stages_json = data.list_files_recursive(
                    os.path.join(data_source_path, app, stage_path), extension="json"
                )

                for count, json_stage in enumerate(list_stages_json):
                    try:
                        with open(json_stage, "r") as data_stage_file:
                            json_data = data_stage_file.read()

                        json_stage_content = json.loads(json_data)

                        status = json_stage_content[0]["status"]

                        if status != "COMPLETE":
                            logger.warning(f"Removing stage not complete ({status}) for application {spark_app_id}")
                            data.delete_file(json_stage)

                        if "stageId" not in json_stage_content[0]:
                            break

                        stage_id = [json_stage_content[0]["stageId"]]

                        data_stage = {
                            key: [json_stage_content[0].get(key)] for key in config.get("internal_vector_stage_columns")
                        }
                        data_stage["sparkAppId"] = [spark_app_id]
                        data_stage["sparkAppName"] = [spark_app_name]
                        data_stage["sparkTags"] = [spark_tags]
                        data_stage["configFile"] = [config_file]
                        data_stage["sparkQueue"] = [spark_queue]
                        data_stage["sparkSchedulerMinRegisteredResourcesRatio"] = [
                            spark_scheduler_minregisteredresourcesratio
                        ]
                        data_stage["sparkMemoryOffHeapSize"] = [spark_memory_offheap_size]
                        data_stage["sparkExecutorInstances"] = [spark_executor_instances]
                        data_stage["sparkExecutorCores"] = [spark_executor_cores]
                        data_stage["sparkExecutorMemory"] = [spark_executor_memory]
                        data_stage["sparkDriverCores"] = [spark_driver_cores]
                        data_stage["sparkDriverMemory"] = [spark_driver_memory]
                        data_stage["sparkDriverMaxResultSize"] = [spark_driver_maxresultsize]
                        data_stage["sparkDefaultParallelism"] = [spark_default_parallelism]
                        data_stage["sparkSqlShufflePartitions"] = [spark_sql_shuffle_partitions]
                        data_stage["sparkShuffleFileBuffer"] = [spark_shuffle_file_buffer]
                        data_stage["dynamicAllocationEnabled"] = [spark_dynamic_allocation]
                        data_stage["dynamicAllocationInitialExecutors"] = [spark_dynamic_allocation_initial]
                        data_stage["dynamicAllocationMinExecutors"] = [spark_dynamic_allocation_min]
                        data_stage["dynamicAllocationMaxExecutors"] = [spark_dynamic_allocation_max]
                        data_stage["dynamicAllocationExecutorsIdleTimeout"] = [
                            spark_dynamic_allocation_executors_idle_timeout
                        ]
                        data_stage["reducerMaxSizeInFlight"] = [spark_reducer_maxsizeinflight]
                        data_stage["sqlAutoBroadcastJoinThreshold"] = [spark_sql_autobroadcastjoin]

                        data_stage["userName"] = [spark_user_name]
                        data_stage["numExecutorsAssocStage"] = [len(json_stage_content[0].get("executorSummary", {}))]

                        for key, value_stage in json_stage_content[0].get("tasks", {}).items():
                            json_normalized_stage = pd.json_normalize(value_stage).assign(stageId=stage_id)
                            df_tasks = pd.concat([df_tasks, json_normalized_stage])

                        df_stage = pd.concat([df_stage, pd.DataFrame(data_stage)])

                    except Exception as e:
                        logger.error(f"Error processing file {json_stage}: {str(e)}")

                try:
                    agg_dict = {
                        key: config.get("internal_vector_tasks_aggegation_metrics")
                        for key in config.get("internal_vector_tasks_agg_columns")
                    }
                    df_tasks_agg = df_tasks.groupby(["stageId"]).agg(agg_dict)
                    df_tasks_agg.columns = [f"{col[0]}_{col[1]}_agg" for col in df_tasks_agg.columns]

                    df_combined = pd.merge(df_stage, df_tasks_agg, on="stageId", how="inner", validate="one_to_one")
                    df_combined = df_combined.reset_index()

                    df_combined["firstTaskLaunchedTime"] = df_combined["firstTaskLaunchedTime"].apply(
                        lambda x: data.convert_date_to_epoch(str(x))
                    )
                    df_combined["completionTime"] = df_combined["completionTime"].apply(
                        lambda x: data.convert_date_to_epoch(str(x))
                    )

                    df_combined["totalTimeSec"] = df_combined.apply(
                        lambda row: abs(row["completionTime"] - row["firstTaskLaunchedTime"]), axis=1
                    )

                    df_combined.fillna(0, inplace=True)

                    df_combined = df_combined.rename(columns=lambda x: x.replace(".", "_"))

                    df_combined = df_combined.map(lambda x: round(x, 2) if isinstance(x, float) else x)

                    path_vector_path_app = os.path.join(path_vector, f"{app}.csv")
                    df_combined.to_csv(path_vector_path_app, index=False)

                    logger.info(f"Processed and saved data for {app} to {path_vector_path_app}")

                except Exception as e:
                    logger.error(f"Error processing aggregation file {json_stage}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"General error in build_vector: {str(e)}")

        end_time = time()
        total_time = end_time - init_time

        logger.info(f"Time processing for {list_apps}: {total_time}")

        return result_apps

    def milvus_load_data(self, path_files):
        process_name = config.get("internal_process_name")
        spark_collection = data.convert_to_camel_case(
            f"{config.get('internal_milvus_collection_spark_metrics')}-{process_name}"
        )
        spark_fields = config.get("internal_milvus_fields_spark_metrics")
        milvus_force_rebuild_schema = config.get("internal_milvus_force_rebuild_schema")

        OUTPUT_FILE = "merge_application"

        self.milvus.connect()

        logger.info(f"Collections{str(self.milvus.list_collections())}")

        if milvus_force_rebuild_schema:
            if self.milvus.has_collection(spark_collection):
                self.milvus.drop_collection(spark_collection)
            self.milvus.create_collection(spark_collection, spark_fields)
        else:
            self.milvus.get_collection(spark_collection)

        all_files = glob.glob(os.path.join(path_files, "*.csv"))

        embedding_instance = Embeddings(model_name=config.get("internal_milvus_model_embeddins"))

        exclude_own_langchain = config.get("internal_milvus_fields_exclude_own_langchain")
        list_field_schema = [item for item in spark_fields if item not in exclude_own_langchain]

        bulk_num_files = config.get("internal_milvuls_bulk_num_csv")

        for i in range(0, len(all_files), bulk_num_files):
            batch = all_files[i : i + bulk_num_files]

            random_string = data.generate_random_string(4)

            num_rows = self.read_and_concatenate_batch(batch, f"{path_files}/{OUTPUT_FILE}_{random_string}.csv")

            data_vector = embedding_instance.build_entities(
                f"{path_files}/{OUTPUT_FILE}_{random_string}.csv", list_field_schema
            )

            logger.info(f"Inserting {num_rows} rows")
            self.milvus.insert_data(data_vector)

        if milvus_force_rebuild_schema:
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

    def read_and_concatenate_batch(self, batch, path_csv):
        # Read and concatenate data from CSV files in the batch
        batch_dataframes = [pd.read_csv(file_path) for file_path in batch]
        concatenated_data = pd.concat(batch_dataframes, ignore_index=True)
        concatenated_data.to_csv(path_csv, index=False)
        return len(concatenated_data)

    def milvus_load_collection(self):
        self.milvus.connect()

        process_name = config.get("internal_process_name")
        spark_collection = data.convert_to_camel_case(
            f"{config.get('internal_milvus_collection_spark_metrics')}-{process_name}"
        )

        self.milvus.get_collection(spark_collection)

        logger.info(f"Loading collection {spark_collection}")

        self.milvus.load_collection()

        logger.info(f"Number of entities in collection: {self.milvus.get_entity_num()}")

        logger.info("End process")
