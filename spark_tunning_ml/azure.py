import concurrent.futures
import os
from concurrent.futures import ThreadPoolExecutor

from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from spark_tunning_ml.logger import logger


class AzureBlobStorageHandler:
    def __init__(self, connection_string, container_name):
        """
        Initializes an instance of the class.

        Args:
            connection_string (str): The connection string to the Azure Blob storage account.
            container_name (str): The name of the container within the Azure Blob storage account.

        Raises:
            AzureError: If there is an error initializing the AzureBlobStorageHandler.

        Returns:
            None
        """
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            self.container_name = container_name
        except AzureError as ex:
            logger.error(f"Error initializing AzureBlobStorageHandler: {ex}")
            raise

    def download_blob(self, blob_name, destination_file_path):
        """
        Downloads a blob from the Azure Blob Storage container.

        Args:
            blob_name (str): The name of the blob to download.
            destination_file_path (str): The path to save the downloaded blob.

        Returns:
            bool: True if the blob was successfully downloaded, False otherwise.
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client(blob_name)

            with open(destination_file_path, "wb") as file:
                data = blob_client.download_blob()
                data.readinto(file)

            logger.info(f"File {blob_name} downloaded to {destination_file_path}")
            return True
        except AzureError as ex:
            logger.error(f"Error downloading blob {blob_name}: {ex}")
            return False

    def download_blobs_in_folder(self, blob_prefix, local_folder_path, file_filter=None, max_workers=5):
        """
        Downloads blobs from a specified folder in the blob storage to a local folder.

        :param blob_prefix: The prefix of the blob names in the storage to filter the blobs to be downloaded.
        :type blob_prefix: str
        :param local_folder_path: The path of the local folder where the blobs will be downloaded.
        :type local_folder_path: str
        :param file_filter: Optional. The file extension filter to only download files with a specific extension.
        :type file_filter: str, optional
        :param max_workers: Optional. The maximum number of worker threads to use for concurrent downloads. Default is 5.
        :type max_workers: int, optional
        :return: True if all blobs are successfully downloaded, False otherwise.
        :rtype: bool
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blobs = container_client.list_blobs(name_starts_with=blob_prefix)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for blob in blobs:
                    blob_name = blob.name
                    if file_filter is None or blob_name.endswith(file_filter):
                        destination_file_path = os.path.join(local_folder_path, blob_name)
                        future = executor.submit(self.download_blob, blob_name, destination_file_path)
                        futures.append(future)

                # Wait for all tasks to complete
                concurrent.futures.wait(futures)
                results = [future.result() for future in futures]
                return all(results)
        except AzureError as ex:
            logger.error(f"Error downloading blobs in folder {blob_prefix}: {ex}")
            return False

    def upload_blob(self, local_file_path, blob_name):
        """
        Uploads a file from the local file system to Azure Blob Storage.

        Args:
            local_file_path (str): The path of the local file to upload.
            blob_name (str): The name of the blob in Azure Blob Storage.

        Returns:
            bool: True if the file was successfully uploaded, False otherwise.
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client(blob_name)

            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            logger.info(f"File {local_file_path} uploaded as {blob_name}")
            return True
        except AzureError as ex:
            logger.error(f"Error uploading blob {blob_name}: {ex}")
            return False

    def upload_blobs(self, files, max_workers=5):
        """
        Uploads multiple blobs to a container in the Azure Blob Storage.

        Args:
            files (List[str]): A list of local file paths to be uploaded as blobs.
            max_workers (int): The maximum number of concurrent workers to use for uploading the blobs.
                Defaults to 5.

        Returns:
            List[Tuple[str, Union[str, bool]]]: A list of tuples containing the file name and the upload result.
                The upload result can be either the blob URL (str) or False if the upload failed.
        """
        try:
            self.blob_service_client.get_container_client(self.container_name)
            results = []

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for local_file_path in files:
                    blob_name = local_file_path

                    future = executor.submit(self.upload_blob, local_file_path, blob_name)
                    futures.append((local_file_path, future))

                # Wait for all tasks to complete
                for local_file_path, future in futures:
                    try:
                        result = future.result()
                        results.append((local_file_path.split("/")[3], result))
                    except Exception as e:
                        logger.error(f"Error uploading file {local_file_path}: {e}")
                        results.append((local_file_path.split("/")[3], False))

            return results
        except AzureError as ex:
            logger.error(f"Error uploading blobs: {ex}")
            return []

    def check_container_exists(self):
        """
        Check if the container exists.

        :return: True if the container exists, False otherwise.
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            container_client.get_container_properties()
            return True
        except ResourceNotFoundError:
            return False

    def create_container(self):
        """
        Creates a container in the Azure Blob Storage if it does not already exist.

        Returns:
            bool: True if the container is created successfully or already exists. False otherwise.
        """
        try:
            if self.check_container_exists():
                logger.info(f"Container '{self.container_name}' already exists.")
                return True
            else:
                container_client = self.blob_service_client.get_container_client(self.container_name)
                container_client.create_container()
                return True
        except AzureError:
            logger.info(f"Error creating container '{self.container_name}'")

    def drop_container(self):
        """
        Deletes the container specified by `self.container_name`.

        Returns:
            - `True` if the container was successfully deleted.
            - `False` if the container was not found.
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            container_client.delete_container()
            logger.info(f"Container '{self.container_name}' deleted.")
            return True
        except ResourceNotFoundError:
            logger.info(f"Container '{self.container_name}' not found.")
            return False

    def remove_blob(self, blob_name):
        """
        Removes a blob from the container.

        Args:
            blob_name (str): The name of the blob to be removed.

        Returns:
            bool: True if the blob is successfully deleted, False otherwise.
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
            logger.info(f"Blob {blob_name} deleted.")
            return True
        except ResourceNotFoundError:
            logger.error(f"Blob {blob_name} not found.")
            return False

    def remove_all_objects(self, max_workers=5):
        """
        Remove all objects in the container.

        :param max_workers: The maximum number of workers to use for concurrent removal (default is 5).
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blobs = container_client.list_blobs()

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.remove_blob, blob.name): blob.name for blob in blobs}

                for future in concurrent.futures.as_completed(futures):
                    blob_name = futures[future]
                    try:
                        result = future.result()
                        if result:
                            logger.info(f"Blob {blob_name} removed.")
                    except Exception as e:
                        logger.error(f"Error removing blob {blob_name}: {e}")

            logger.info(f"All objects in container '{self.container_name}' removed.")
        except ResourceNotFoundError:
            logger.error(f"Container '{self.container_name}' does not exist.")
