from pymilvus import Collection, CollectionSchema, DataType, FieldSchema, MilvusException, connections, utility

from spark_tunning_ml.logger import logger


class MilvusWrapper:
    def __init__(self, host="localhost", port=19530):
        self.host = host
        self.port = port
        self.collection = None

    def connect(self):
        try:
            connections.connect(host=self.host, port=self.port)
        except MilvusException as e:
            logger.error(f"Failed to connect to Milvus: {e}")

    def disconnect(self):
        try:
            connections.disconnect()
        except MilvusException as e:
            logger.error(f"Failed to disconnect from Milvus: {e}")

    def create_collection(
        self,
        collection_name,
        fields,
    ):
        if self.collection:
            raise MilvusException("Collection already exists.")

        fields_list = []

        for field_name, field_info in fields.items():
            if field_info["type"] == "FLOAT_VECTOR":
                fields_list.append(
                    FieldSchema(
                        name=field_name,
                        dtype=DataType[field_info["type"]],
                        dim=field_info["dim"],
                        description=field_info.get("description", ""),
                    )
                )
            else:
                fields_list.append(
                    FieldSchema(
                        name=field_name,
                        is_primary=field_info.get("is_primary", False),
                        dtype=DataType[field_info["type"]],
                        auto_id=field_info.get("auto_id", False),
                        description=field_info.get("description", ""),
                        max_length=field_info.get("max_length", 200),
                    )
                )

        schema = CollectionSchema(
            fields=fields_list,
            enable_dynamic_field=True,
        )

        collection = Collection(
            name=collection_name,
            schema=schema,
            using="default",
            shards_num=2,
            consistency_level="Session",
        )
        self.collection = collection

    def get_collection(self, collection_name):
        self.collection = Collection(collection_name)
        return self.collection

    def has_collection(self, collection_name):
        return utility.has_collection(collection_name)

    def drop_collection(self, collection_name):
        return utility.drop_collection(collection_name)

    def list_collections(self):
        return utility.list_collections()

    def get_entity_num(self):
        return self.collection.num_entities

    def has_index(self):
        return self.collection.has_index()

    def load_collection(self):
        self.collection.load()

    def release_collection(self):
        self.collection.release()

    def check_connection(self):
        """
        Check the connection status.

        Returns:
            bool: True if the connection is established, False otherwise.
        """
        if connections.is_connected():
            return True
        else:
            logger.error("Failed to check connection")
            return False

    def search(self, data, anns_field, param, limit, expr, output_fields):
        """
        Performs a search operation on the collection.

        Args:
            data (list): The list of vectors to be searched.
            anns_field (str): The field used for ANN search.
            param (dict): The parameters for the search operation.
            limit (int): The maximum number of search results to return.
            expr (str): The expression used for advanced search.
            output_fields (list): The list of fields to be returned in the search results.

        Returns:
            list: The list of search results.
        """
        if not self.collection:
            raise MilvusException("Collection not initialized. Create a collection first.")

        results = self.collection.search(
            data=data,
            anns_field=anns_field,
            param=param,
            limit=limit,
            expr=expr,
            output_fields=output_fields,
            consistency_level="Strong",
        )
        return results

    def query(self, expr, offset=0, limit=10, output_fields=None):
        """
        Query the collection using the given expression.

        Args:
            expr (str): The expression to query the collection.
            offset (int, optional): The offset to start returning results from. Defaults to 0.
            limit (int, optional): The maximum number of results to return. Defaults to 10.
            output_fields (List[str], optional): The fields to include in the returned results. Defaults to None.

        Returns:
            List[Dict[str, Any]]: The query results.
        """
        if not self.collection:
            raise MilvusException("Collection not initialized. Create a collection first.")

        results = self.collection.query(expr=expr, offset=offset, limit=limit, output_fields=output_fields)
        return results

    def create_index(self, field_name, index_params=None):
        """
        Create an index on a specified field in the collection.

        Parameters:
            field_name (str): The name of the field on which to create the index.
            index_params (dict, optional): Additional parameters for creating the index. Defaults to None.

        Raises:
            MilvusException: If the collection has not been initialized.

        Returns:
            None
        """
        if not self.collection:
            raise MilvusException("Collection not initialized. Create a collection first.")
        try:
            self.collection.create_index(field_name=field_name, index_params=index_params)
        except MilvusException as e:
            logger.error(f"Failed to create index: {e}")

    def insert_data(self, data):
        result = None

        if not self.collection:
            raise MilvusException("Collection not initialized. Create a collection first.")
        try:
            logger.info("start insert")
            self.collection.insert(data)
            self.collection.flush()
            logger.info("end insert")
        except MilvusException as e:
            logger.error(f"Failed to insert data: {e}")

        return result
