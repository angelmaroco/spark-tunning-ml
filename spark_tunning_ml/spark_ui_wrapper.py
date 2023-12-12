from typing import Dict, List, Union

from spark_tunning_ml.config import config
from spark_tunning_ml.request_wrapper import RequestWrapper
from spark_tunning_ml.schema import SchemaValidator


class SparkUIWrapper:
    def __init__(self, base_url):
        """
        Initialize SparkUIWrapper.

        Args:
            base_url (str): The base URL of the Spark UI.
        """
        self.base_url = base_url
        self.request_wrapper = RequestWrapper(base_url)

    def get_applications(self):
        """
        Fetch information about Spark applications.

        Returns:
            requests.Response: The response object containing the JSON data.
        """

        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "attempts": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "startTime": {"type": "string"},
                                "endTime": {"type": "string"},
                                "lastUpdated": {"type": "string"},
                                "duration": {"type": "number"},
                                "sparkUser": {"type": "string"},
                                "completed": {"type": "boolean"},
                                "appSparkVersion": {"type": "string"},
                                "startTimeEpoch": {"type": "number"},
                                "endTimeEpoch": {"type": "number"},
                                "lastUpdatedEpoch": {"type": "number"},
                            },
                            "required": [
                                "startTime",
                                "endTime",
                                "lastUpdated",
                                "duration",
                                "sparkUser",
                                "completed",
                                "appSparkVersion",
                                "startTimeEpoch",
                                "endTimeEpoch",
                                "lastUpdatedEpoch",
                            ],
                        },
                    },
                },
                "required": ["id", "name", "attempts"],
            },
        }

        endpoint = config.get("spark_ui_api_endpoint_applications")
        applications = self.request_wrapper.request("GET", endpoint)

        schema = SchemaValidator(schema)
        schema.validate(applications)

        return applications

    def get_ids_from_applications(self, applications, filter_completed=False):
        """
        Get a list of IDs from a list of applications.

        Args:
            applications (list): A list of applications.
            filter_completed (bool, optional): If True,  filter only completed applications. Defaults to False.

        Returns:
            list: A list of IDs.
        """

        ids = []

        if filter_completed:
            for app in applications:
                for attempt in app["attempts"]:
                    if attempt.get("completed"):
                        ids = [app["id"]]
                        break
        else:
            ids = [app["id"] for app in applications]

        return ids

    def get_executors(self, app_id):
        """
        Get information about executors for a specific application.

        Args:
            app_id (str): The ID of the application.

        Returns:
            Response: The response object from the executors endpoint.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "hostPort": {"type": "string"},
                    "isActive": {"type": "boolean"},
                    "rddBlocks": {"type": "number"},
                    "memoryUsed": {"type": "number"},
                    "diskUsed": {"type": "number"},
                    "totalCores": {"type": "number"},
                    "maxTasks": {"type": "number"},
                    "activeTasks": {"type": "number"},
                    "failedTasks": {"type": "number"},
                    "completedTasks": {"type": "number"},
                    "totalTasks": {"type": "number"},
                    "totalDuration": {"type": "number"},
                    "totalGCTime": {"type": "number"},
                    "totalInputBytes": {"type": "number"},
                    "totalShuffleRead": {"type": "number"},
                    "totalShuffleWrite": {"type": "number"},
                    "isBlacklisted": {"type": "boolean"},
                    "maxMemory": {"type": "number"},
                    "addTime": {"type": "string"},
                    "executorLogs": {
                        "type": "object",
                        "properties": {
                            "stdout": {"type": "string"},
                            "stderr": {"type": "string"},
                        },
                        "required": [],
                    },
                    "memoryMetrics": {
                        "type": "object",
                        "properties": {
                            "usedOnHeapStorageMemory": {"type": "number"},
                            "usedOffHeapStorageMemory": {"type": "number"},
                            "totalOnHeapStorageMemory": {"type": "number"},
                            "totalOffHeapStorageMemory": {"type": "number"},
                        },
                        "required": [
                            "usedOnHeapStorageMemory",
                            "usedOffHeapStorageMemory",
                            "totalOnHeapStorageMemory",
                            "totalOffHeapStorageMemory",
                        ],
                    },
                    "blacklistedInStages": {"type": "array", "items": {}},
                },
                "required": [
                    "id",
                    "hostPort",
                    "isActive",
                    "rddBlocks",
                    "memoryUsed",
                    "diskUsed",
                    "totalCores",
                    "maxTasks",
                    "activeTasks",
                    "failedTasks",
                    "completedTasks",
                    "totalTasks",
                    "totalDuration",
                    "totalGCTime",
                    "totalInputBytes",
                    "totalShuffleRead",
                    "totalShuffleWrite",
                    "isBlacklisted",
                    "maxMemory",
                    "addTime",
                    "executorLogs",
                    "memoryMetrics",
                    "blacklistedInStages",
                ],
            },
        }

        endpoint = config.get("spark_ui_api_endpoint_executors").format(app_id=app_id)
        executors = self.request_wrapper.request("GET", endpoint)

        schema = SchemaValidator(schema)
        schema.validate(executors)

        return executors

    def get_jobs(self, app_id):
        """
        Get information about jobs for a specific application.

        Args:
            app_id (str): The ID of the application.

        Returns:
            Response: The response object from the jobs endpoint.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "jobId": {"type": "number"},
                    "name": {"type": "string"},
                    "submissionTime": {"type": "string"},
                    "completionTime": {"type": "string"},
                    "stageIds": {"type": "array", "items": {"type": "number"}},
                    "status": {"type": "string"},
                    "numTasks": {"type": "number"},
                    "numActiveTasks": {"type": "number"},
                    "numCompletedTasks": {"type": "number"},
                    "numSkippedTasks": {"type": "number"},
                    "numFailedTasks": {"type": "number"},
                    "numKilledTasks": {"type": "number"},
                    "numCompletedIndices": {"type": "number"},
                    "numActiveStages": {"type": "number"},
                    "numCompletedStages": {"type": "number"},
                    "numSkippedStages": {"type": "number"},
                    "numFailedStages": {"type": "number"},
                    "killedTasksSummary": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                    },
                },
                "required": [
                    "jobId",
                    "name",
                    "submissionTime",
                    "completionTime",
                    "stageIds",
                    "status",
                    "numTasks",
                    "numActiveTasks",
                    "numCompletedTasks",
                    "numSkippedTasks",
                    "numFailedTasks",
                    "numKilledTasks",
                    "numCompletedIndices",
                    "numActiveStages",
                    "numCompletedStages",
                    "numSkippedStages",
                    "numFailedStages",
                    "killedTasksSummary",
                ],
            },
        }

        endpoint = config.get("spark_ui_api_endpoint_jobs").format(app_id=app_id)
        jobs = self.request_wrapper.request("GET", endpoint)

        schema = SchemaValidator(schema)
        schema.validate(jobs)

        return jobs

    def get_job(self, app_id, job_id):
        """
        Get information about a specific job in an application.

        Args:
            app_id (str): The ID of the application.
            job_id (str): The ID of the job.

        Returns:
            Response: The response object from the job endpoint.
        """
        endpoint = f"applications/{app_id}/jobs/{job_id}"
        return self.request_wrapper.request("GET", endpoint)

    def get_stages(self, app_id):
        """
        Get information about stages for a specific application.

        Args:
            app_id (str): The ID of the application.

        Returns:
            Response: The response object from the stages endpoint.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "stageId": {"type": "number"},
                    "attemptId": {"type": "number"},
                    "numTasks": {"type": "number"},
                    "numActiveTasks": {"type": "number"},
                    "numCompleteTasks": {"type": "number"},
                    "numFailedTasks": {"type": "number"},
                    "numKilledTasks": {"type": "number"},
                    "numCompletedIndices": {"type": "number"},
                    "executorRunTime": {"type": "number"},
                    "executorCpuTime": {"type": "number"},
                    "submissionTime": {"type": "string"},
                    "firstTaskLaunchedTime": {"type": "string"},
                    "completionTime": {"type": "string"},
                    "inputBytes": {"type": "number"},
                    "inputRecords": {"type": "number"},
                    "outputBytes": {"type": "number"},
                    "outputRecords": {"type": "number"},
                    "shuffleReadBytes": {"type": "number"},
                    "shuffleReadRecords": {"type": "number"},
                    "shuffleWriteBytes": {"type": "number"},
                    "shuffleWriteRecords": {"type": "number"},
                    "memoryBytesSpilled": {"type": "number"},
                    "diskBytesSpilled": {"type": "number"},
                    "name": {"type": "string"},
                    "details": {"type": "string"},
                    "schedulingPool": {"type": "string"},
                    "rddIds": {"type": "array", "items": {"type": "number"}},
                    "accumulatorUpdates": {"type": "array", "items": {}},
                    "killedTasksSummary": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                    },
                },
                "required": [
                    "status",
                    "stageId",
                    "attemptId",
                    "numTasks",
                    "numActiveTasks",
                    "numCompleteTasks",
                    "numFailedTasks",
                    "numKilledTasks",
                    "numCompletedIndices",
                    "executorRunTime",
                    "executorCpuTime",
                    "submissionTime",
                    "firstTaskLaunchedTime",
                    "completionTime",
                    "inputBytes",
                    "inputRecords",
                    "outputBytes",
                    "outputRecords",
                    "shuffleReadBytes",
                    "shuffleReadRecords",
                    "shuffleWriteBytes",
                    "shuffleWriteRecords",
                    "memoryBytesSpilled",
                    "diskBytesSpilled",
                    "name",
                    "details",
                    "schedulingPool",
                    "rddIds",
                    "accumulatorUpdates",
                    "killedTasksSummary",
                ],
            },
        }

        endpoint = config.get("spark_ui_api_endpoint_stages").format(app_id=app_id)
        stages = self.request_wrapper.request("GET", endpoint)

        schema = SchemaValidator(schema)
        schema.validate(stages)

        return stages

    def get_ids_form_stages(self, stages):
        """
        Get a list of IDs from a list of stages.

        Args:
            stages (list): A list of stages.

        Returns:
            list: A list of IDs.
        """
        return [stage["stageId"] for stage in stages]

    def get_id_from_stage_attempts(self, stage: List[Dict[str, Union[str, int]]]):
        """
        Get the attempt ID from the given stage.

        Args:
            stage (List[Dict[str, Union[str, int]]]): A list of dictionaries representing the stage.

        Returns:
            int: The attempt ID from the stage.
        """
        if stage:
            return stage[0].get("attemptId")
        else:
            return None

    def get_stage(self, app_id, stage_id):
        """
        Get information about a specific stage in an application.

        Args:
            app_id (str): The ID of the application.
            stage_id (str): The ID of the stage.

        Returns:
            Response: The response object from the stage endpoint.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "stageId": {"type": "number"},
                    "attemptId": {"type": "number"},
                    "numTasks": {"type": "number"},
                    "numActiveTasks": {"type": "number"},
                    "numCompleteTasks": {"type": "number"},
                    "numFailedTasks": {"type": "number"},
                    "numKilledTasks": {"type": "number"},
                    "numCompletedIndices": {"type": "number"},
                    "executorRunTime": {"type": "number"},
                    "executorCpuTime": {"type": "number"},
                    "submissionTime": {"type": "string"},
                    "firstTaskLaunchedTime": {"type": "string"},
                    "completionTime": {"type": "string"},
                    "inputBytes": {"type": "number"},
                    "inputRecords": {"type": "number"},
                    "outputBytes": {"type": "number"},
                    "outputRecords": {"type": "number"},
                    "shuffleReadBytes": {"type": "number"},
                    "shuffleReadRecords": {"type": "number"},
                    "shuffleWriteBytes": {"type": "number"},
                    "shuffleWriteRecords": {"type": "number"},
                    "memoryBytesSpilled": {"type": "number"},
                    "diskBytesSpilled": {"type": "number"},
                    "name": {"type": "string"},
                    "details": {"type": "string"},
                    "schedulingPool": {"type": "string"},
                    "rddIds": {"type": "array", "items": {"type": "number"}},
                    "accumulatorUpdates": {"type": "array", "items": {}},
                    "tasks": {
                        "type": "object",
                        "properties": {
                            "1": {
                                "type": "object",
                                "properties": {
                                    "taskId": {"type": "number"},
                                    "index": {"type": "number"},
                                    "attempt": {"type": "number"},
                                    "launchTime": {"type": "string"},
                                    "duration": {"type": "number"},
                                    "executorId": {"type": "string"},
                                    "host": {"type": "string"},
                                    "status": {"type": "string"},
                                    "taskLocality": {"type": "string"},
                                    "speculative": {"type": "boolean"},
                                    "accumulatorUpdates": {
                                        "type": "array",
                                        "items": {},
                                    },
                                    "taskMetrics": {
                                        "type": "object",
                                        "properties": {
                                            "executorDeserializeTime": {
                                                "type": "number"
                                            },
                                            "executorDeserializeCpuTime": {
                                                "type": "number"
                                            },
                                            "executorRunTime": {"type": "number"},
                                            "executorCpuTime": {"type": "number"},
                                            "resultSize": {"type": "number"},
                                            "jvmGcTime": {"type": "number"},
                                            "resultSerializationTime": {
                                                "type": "number"
                                            },
                                            "memoryBytesSpilled": {"type": "number"},
                                            "diskBytesSpilled": {"type": "number"},
                                            "peakExecutionMemory": {"type": "number"},
                                            "inputMetrics": {
                                                "type": "object",
                                                "properties": {
                                                    "bytesRead": {"type": "number"},
                                                    "recordsRead": {"type": "number"},
                                                },
                                                "required": [
                                                    "bytesRead",
                                                    "recordsRead",
                                                ],
                                            },
                                            "outputMetrics": {
                                                "type": "object",
                                                "properties": {
                                                    "bytesWritten": {"type": "number"},
                                                    "recordsWritten": {
                                                        "type": "number"
                                                    },
                                                },
                                                "required": [
                                                    "bytesWritten",
                                                    "recordsWritten",
                                                ],
                                            },
                                            "shuffleReadMetrics": {
                                                "type": "object",
                                                "properties": {
                                                    "remoteBlocksFetched": {
                                                        "type": "number"
                                                    },
                                                    "localBlocksFetched": {
                                                        "type": "number"
                                                    },
                                                    "fetchWaitTime": {"type": "number"},
                                                    "remoteBytesRead": {
                                                        "type": "number"
                                                    },
                                                    "remoteBytesReadToDisk": {
                                                        "type": "number"
                                                    },
                                                    "localBytesRead": {
                                                        "type": "number"
                                                    },
                                                    "recordsRead": {"type": "number"},
                                                },
                                                "required": [
                                                    "remoteBlocksFetched",
                                                    "localBlocksFetched",
                                                    "fetchWaitTime",
                                                    "remoteBytesRead",
                                                    "remoteBytesReadToDisk",
                                                    "localBytesRead",
                                                    "recordsRead",
                                                ],
                                            },
                                            "shuffleWriteMetrics": {
                                                "type": "object",
                                                "properties": {
                                                    "bytesWritten": {"type": "number"},
                                                    "writeTime": {"type": "number"},
                                                    "recordsWritten": {
                                                        "type": "number"
                                                    },
                                                },
                                                "required": [
                                                    "bytesWritten",
                                                    "writeTime",
                                                    "recordsWritten",
                                                ],
                                            },
                                        },
                                        "required": [
                                            "executorDeserializeTime",
                                            "executorDeserializeCpuTime",
                                            "executorRunTime",
                                            "executorCpuTime",
                                            "resultSize",
                                            "jvmGcTime",
                                            "resultSerializationTime",
                                            "memoryBytesSpilled",
                                            "diskBytesSpilled",
                                            "peakExecutionMemory",
                                            "inputMetrics",
                                            "outputMetrics",
                                            "shuffleReadMetrics",
                                            "shuffleWriteMetrics",
                                        ],
                                    },
                                },
                                "required": [
                                    "taskId",
                                    "index",
                                    "attempt",
                                    "launchTime",
                                    "duration",
                                    "executorId",
                                    "host",
                                    "status",
                                    "taskLocality",
                                    "speculative",
                                    "accumulatorUpdates",
                                ],
                            }
                        },
                    },
                    "executorSummary": {
                        "type": "object",
                        "properties": {
                            "1": {
                                "type": "object",
                                "properties": {
                                    "taskTime": {"type": "number"},
                                    "failedTasks": {"type": "number"},
                                    "succeededTasks": {"type": "number"},
                                    "killedTasks": {"type": "number"},
                                    "inputBytes": {"type": "number"},
                                    "inputRecords": {"type": "number"},
                                    "outputBytes": {"type": "number"},
                                    "outputRecords": {"type": "number"},
                                    "shuffleRead": {"type": "number"},
                                    "shuffleReadRecords": {"type": "number"},
                                    "shuffleWrite": {"type": "number"},
                                    "shuffleWriteRecords": {"type": "number"},
                                    "memoryBytesSpilled": {"type": "number"},
                                    "diskBytesSpilled": {"type": "number"},
                                    "isBlacklistedForStage": {"type": "boolean"},
                                },
                                "required": [
                                    "taskTime",
                                    "failedTasks",
                                    "succeededTasks",
                                    "killedTasks",
                                    "inputBytes",
                                    "inputRecords",
                                    "outputBytes",
                                    "outputRecords",
                                    "shuffleRead",
                                    "shuffleReadRecords",
                                    "shuffleWrite",
                                    "shuffleWriteRecords",
                                    "memoryBytesSpilled",
                                    "diskBytesSpilled",
                                    "isBlacklistedForStage",
                                ],
                            }
                        },
                    },
                    "killedTasksSummary": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                    },
                },
                "required": [
                    "status",
                    "stageId",
                    "attemptId",
                    "numTasks",
                    "numActiveTasks",
                    "numCompleteTasks",
                    "numFailedTasks",
                    "numKilledTasks",
                    "numCompletedIndices",
                    "executorRunTime",
                    "executorCpuTime",
                    "submissionTime",
                    "firstTaskLaunchedTime",
                    "completionTime",
                    "inputBytes",
                    "inputRecords",
                    "outputBytes",
                    "outputRecords",
                    "shuffleReadBytes",
                    "shuffleReadRecords",
                    "shuffleWriteBytes",
                    "shuffleWriteRecords",
                    "memoryBytesSpilled",
                    "diskBytesSpilled",
                    "name",
                    "details",
                    "schedulingPool",
                    "rddIds",
                    "accumulatorUpdates",
                    "tasks",
                    "executorSummary",
                    "killedTasksSummary",
                ],
            },
        }

        endpoint = config.get("spark_ui_api_endpoint_stage").format(
            app_id=app_id, stage_id=stage_id
        )
        stage = self.request_wrapper.request("GET", endpoint)

        schema = SchemaValidator(schema)
        schema.validate(stage)

        return stage

    def get_stage_attempt(self, app_id, stage_id, stage_attempt_id):
        """
        Get information about a specific stage attempt in an application.

        Args:
            app_id (str): The ID of the application.
            stage_id (str): The ID of the stage.
            stage_attempt_id (str): The ID of the stage attempt.

        Returns:
            Response: The response object from the stage attempt endpoint.
        """
        endpoint = f"applications/{app_id}/stages/{stage_id}/{stage_attempt_id}"
        return self.request_wrapper.request("GET", endpoint)

    def get_task_summary(self, app_id, stage_id, stage_attempt_id):
        """
        Get task summary information for a specific stage attempt in an application.

        Args:
            app_id (str): The ID of the application.
            stage_id (str): The ID of the stage.
            stage_attempt_id (str): The ID of the stage attempt.

        Returns:
            Response: The response object from the task summary endpoint.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "object",
            "properties": {
                "quantiles": {"type": "array", "items": {"type": "number"}},
                "executorDeserializeTime": {
                    "type": "array",
                    "items": {"type": "number"},
                },
                "executorDeserializeCpuTime": {
                    "type": "array",
                    "items": {"type": "number"},
                },
                "executorRunTime": {"type": "array", "items": {"type": "number"}},
                "executorCpuTime": {"type": "array", "items": {"type": "number"}},
                "resultSize": {"type": "array", "items": {"type": "number"}},
                "jvmGcTime": {"type": "array", "items": {"type": "number"}},
                "resultSerializationTime": {
                    "type": "array",
                    "items": {"type": "number"},
                },
                "gettingResultTime": {"type": "array", "items": {"type": "number"}},
                "schedulerDelay": {"type": "array", "items": {"type": "number"}},
                "peakExecutionMemory": {"type": "array", "items": {"type": "number"}},
                "memoryBytesSpilled": {"type": "array", "items": {"type": "number"}},
                "diskBytesSpilled": {"type": "array", "items": {"type": "number"}},
                "inputMetrics": {
                    "type": "object",
                    "properties": {
                        "bytesRead": {"type": "array", "items": {"type": "number"}},
                        "recordsRead": {"type": "array", "items": {"type": "number"}},
                    },
                    "required": ["bytesRead", "recordsRead"],
                },
                "outputMetrics": {
                    "type": "object",
                    "properties": {
                        "bytesWritten": {"type": "array", "items": {"type": "number"}},
                        "recordsWritten": {
                            "type": "array",
                            "items": {"type": "number"},
                        },
                    },
                    "required": ["bytesWritten", "recordsWritten"],
                },
                "shuffleReadMetrics": {
                    "type": "object",
                    "properties": {
                        "readBytes": {"type": "array", "items": {"type": "number"}},
                        "readRecords": {"type": "array", "items": {"type": "number"}},
                        "remoteBlocksFetched": {
                            "type": "array",
                            "items": {"type": "number"},
                        },
                        "localBlocksFetched": {
                            "type": "array",
                            "items": {"type": "number"},
                        },
                        "fetchWaitTime": {"type": "array", "items": {"type": "number"}},
                        "remoteBytesRead": {
                            "type": "array",
                            "items": {"type": "number"},
                        },
                        "remoteBytesReadToDisk": {
                            "type": "array",
                            "items": {"type": "number"},
                        },
                        "totalBlocksFetched": {
                            "type": "array",
                            "items": {"type": "number"},
                        },
                    },
                    "required": [
                        "readBytes",
                        "readRecords",
                        "remoteBlocksFetched",
                        "localBlocksFetched",
                        "fetchWaitTime",
                        "remoteBytesRead",
                        "remoteBytesReadToDisk",
                        "totalBlocksFetched",
                    ],
                },
                "shuffleWriteMetrics": {
                    "type": "object",
                    "properties": {
                        "writeBytes": {"type": "array", "items": {"type": "number"}},
                        "writeRecords": {"type": "array", "items": {"type": "number"}},
                        "writeTime": {"type": "array", "items": {"type": "number"}},
                    },
                    "required": ["writeBytes", "writeRecords", "writeTime"],
                },
            },
            "required": [
                "quantiles",
                "executorDeserializeTime",
                "executorDeserializeCpuTime",
                "executorRunTime",
                "executorCpuTime",
                "resultSize",
                "jvmGcTime",
                "resultSerializationTime",
                "gettingResultTime",
                "schedulerDelay",
                "peakExecutionMemory",
                "memoryBytesSpilled",
                "diskBytesSpilled",
                "inputMetrics",
                "outputMetrics",
                "shuffleReadMetrics",
                "shuffleWriteMetrics",
            ],
        }

        endpoint = config.get("spark_ui_api_endpoint_stage_tasksummary").format(
            app_id=app_id, stage_id=stage_id, stage_attempt_id=stage_attempt_id
        )
        tasksummary = self.request_wrapper.request("GET", endpoint)

        schema = SchemaValidator(schema)
        schema.validate(tasksummary)

        return tasksummary

    def get_task_list(self, app_id, stage_id, stage_attempt_id):
        """
        Get task list information for a specific stage attempt in an application.

        Args:
            app_id (str): The ID of the application.
            stage_id (str): The ID of the stage.
            stage_attempt_id (str): The ID of the stage attempt.

        Returns:
            Response: The response object from the task list endpoint.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "taskId": {"type": "number"},
                    "index": {"type": "number"},
                    "attempt": {"type": "number"},
                    "launchTime": {"type": "string"},
                    "duration": {"type": "number"},
                    "executorId": {"type": "string"},
                    "host": {"type": "string"},
                    "status": {"type": "string"},
                    "taskLocality": {"type": "string"},
                    "speculative": {"type": "boolean"},
                    "accumulatorUpdates": {"type": "array", "items": {}},
                    "taskMetrics": {
                        "type": "object",
                        "properties": {
                            "executorDeserializeTime": {"type": "number"},
                            "executorDeserializeCpuTime": {"type": "number"},
                            "executorRunTime": {"type": "number"},
                            "executorCpuTime": {"type": "number"},
                            "resultSize": {"type": "number"},
                            "jvmGcTime": {"type": "number"},
                            "resultSerializationTime": {"type": "number"},
                            "memoryBytesSpilled": {"type": "number"},
                            "diskBytesSpilled": {"type": "number"},
                            "peakExecutionMemory": {"type": "number"},
                            "inputMetrics": {
                                "type": "object",
                                "properties": {
                                    "bytesRead": {"type": "number"},
                                    "recordsRead": {"type": "number"},
                                },
                                "required": ["bytesRead", "recordsRead"],
                            },
                            "outputMetrics": {
                                "type": "object",
                                "properties": {
                                    "bytesWritten": {"type": "number"},
                                    "recordsWritten": {"type": "number"},
                                },
                                "required": ["bytesWritten", "recordsWritten"],
                            },
                            "shuffleReadMetrics": {
                                "type": "object",
                                "properties": {
                                    "remoteBlocksFetched": {"type": "number"},
                                    "localBlocksFetched": {"type": "number"},
                                    "fetchWaitTime": {"type": "number"},
                                    "remoteBytesRead": {"type": "number"},
                                    "remoteBytesReadToDisk": {"type": "number"},
                                    "localBytesRead": {"type": "number"},
                                    "recordsRead": {"type": "number"},
                                },
                                "required": [
                                    "remoteBlocksFetched",
                                    "localBlocksFetched",
                                    "fetchWaitTime",
                                    "remoteBytesRead",
                                    "remoteBytesReadToDisk",
                                    "localBytesRead",
                                    "recordsRead",
                                ],
                            },
                            "shuffleWriteMetrics": {
                                "type": "object",
                                "properties": {
                                    "bytesWritten": {"type": "number"},
                                    "writeTime": {"type": "number"},
                                    "recordsWritten": {"type": "number"},
                                },
                                "required": [
                                    "bytesWritten",
                                    "writeTime",
                                    "recordsWritten",
                                ],
                            },
                        },
                        "required": [
                            "executorDeserializeTime",
                            "executorDeserializeCpuTime",
                            "executorRunTime",
                            "executorCpuTime",
                            "resultSize",
                            "jvmGcTime",
                            "resultSerializationTime",
                            "memoryBytesSpilled",
                            "diskBytesSpilled",
                            "peakExecutionMemory",
                            "inputMetrics",
                            "outputMetrics",
                            "shuffleReadMetrics",
                            "shuffleWriteMetrics",
                        ],
                    },
                },
                "required": [
                    "taskId",
                    "index",
                    "attempt",
                    "launchTime",
                    "duration",
                    "executorId",
                    "host",
                    "status",
                    "taskLocality",
                    "speculative",
                    "accumulatorUpdates",
                ],
            },
        }

        endpoint = config.get("spark_ui_api_endpoint_stage_tasklist").format(
            app_id=app_id, stage_id=stage_id, stage_attempt_id=stage_attempt_id
        )
        tasklist = self.request_wrapper.request("GET", endpoint)

        schema = SchemaValidator(schema)
        schema.validate(tasklist)

        return tasklist

    def get_executor_threads(self, app_id, executor_id):
        """
        Get information about threads for a specific executor in an application.

        Args:
            app_id (str): The ID of the application.
            executor_id (str): The ID of the executor.

        Returns:
            Response: The response object from the executor threads endpoint.
        """
        endpoint = f"applications/{app_id}/executors/{executor_id}/threads"
        return self.request_wrapper.request("GET", endpoint)

    def get_all_executors(self, app_id):
        """
        Get information about all executors for a specific application.

        Args:
            app_id (str): The ID of the application.

        Returns:
            Response: The response object from the all executors endpoint.
        """
        endpoint = f"applications/{app_id}/allexecutors"
        return self.request_wrapper.request("GET", endpoint)

    def get_storage_rdd(self, app_id):
        """
        Get information about RDD storage for a specific application.

        Args:
            app_id (str): The ID of the application.

        Returns:
            Response: The response object from the storage RDD endpoint.
        """
        endpoint = f"applications/{app_id}/storage/rdd"
        return self.request_wrapper.request("GET", endpoint)

    def get_specific_storage_rdd(self, app_id, rdd_id):
        """
        Get information about a specific RDD storage for a specific application.

        Args:
            app_id (str): The ID of the application.
            rdd_id (str): The ID of the RDD.

        Returns:
            Response: The response object from the specific storage RDD endpoint.
        """
        endpoint = f"applications/{app_id}/storage/rdd/{rdd_id}"
        return self.request_wrapper.request("GET", endpoint)

    def get_logs(self, app_id):
        """
        Get logs for a specific application.

        Args:
            app_id (str): The ID of the application.

        Returns:
            Response: The response object from the logs endpoint.
        """
        endpoint = f"applications/{app_id}/logs"
        return self.request_wrapper.request("GET", endpoint)

    def get_attempt_logs(self, base_app_id, attempt_id):
        """
        Get logs for a specific application attempt.

        Args:
            base_app_id (str): The base ID of the application.
            attempt_id (str): The ID of the attempt.

        Returns:
            Response: The response object from the attempt logs endpoint.
        """
        endpoint = f"applications/{base_app_id}/{attempt_id}/logs"
        return self.request_wrapper.request("GET", endpoint)

    def get_environment(self, app_id):
        """
        Get environment information for a specific application.

        Args:
            app_id (str): The ID of the application.

        Returns:
            Response: The response object from the environment endpoint.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "object",
            "properties": {
                "runtime": {
                    "type": "object",
                    "properties": {
                        "javaVersion": {"type": "string"},
                        "javaHome": {"type": "string"},
                        "scalaVersion": {"type": "string"},
                    },
                    "required": ["javaVersion", "javaHome", "scalaVersion"],
                },
                "sparkProperties": {
                    "type": "array",
                    "items": {"type": "array", "items": {"type": "string"}},
                },
                "systemProperties": {
                    "type": "array",
                    "items": {"type": "array", "items": {"type": "string"}},
                },
                "classpathEntries": {
                    "type": "array",
                    "items": {"type": "array", "items": {"type": "string"}},
                },
            },
            "required": [
                "runtime",
                "sparkProperties",
                "systemProperties",
                "classpathEntries",
            ],
        }

        endpoint = config.get("spark_ui_api_endpoint_environment").format(app_id=app_id)
        environment = self.request_wrapper.request("GET", endpoint)

        schema = SchemaValidator(schema)
        schema.validate(environment)

        return environment

    def get_environment_spark_properties(self, environment_info):
        """
        Get Spark properties from the environment information.

        Args:
            environment_info (dict): The environment information.

        Returns:
            list: A list of Spark properties.
        """
        spark_properties = environment_info[0]["sparkProperties"]
        spark_properties_list = {}

        for properties in spark_properties:
            spark_properties_list[properties[0]] = properties[1]

        return spark_properties_list

    def get_version(self):
        """
        Get the version information using a GET request.

        Returns:
            dict: A dictionary containing version information.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "object",
            "properties": {"spark": {"type": "string"}},
            "required": ["spark"],
        }

        endpoint = config.get("spark_ui_api_endpoint_version")
        version = self.request_wrapper.request("GET", endpoint)

        schema = SchemaValidator(schema)
        schema.validate(version)

        return version
