import pandas as pd
import torch.backends
import torch.cuda
from langchain.embeddings.huggingface import HuggingFaceEmbeddings
from pandarallel import pandarallel

from spark_tunning_ml.logger import logger

pandarallel.initialize()

EMBEDDING_DEVICE = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"


class Embeddings:
    def __init__(self, model_name="all-MiniLM-L6-v2"):
        self.model_name = model_name
        self.embedding = HuggingFaceEmbeddings(model_name=self.model_name, model_kwargs={"device": EMBEDDING_DEVICE})

    def get_model_name(self):
        return self.model_name

    def set_model_name(self, model_name):
        self.model_name = model_name

    def get_key_value_pairs(self, row):
        return ", ".join([f"{key}: {value}" for key, value in row.items()])

    def build_entities(self, file_path="", list_fields_schema=[]):
        """
        Build embeddings for list of files
        """
        logger.info("Starting build embeddings")
        df = pd.read_csv(file_path)

        logger.info("Calculating combined field")
        df["text"] = df.parallel_apply(lambda row: self.get_key_value_pairs(row), axis=1)

        logger.info("Calculating vector field")
        df["vector"] = self.get_data_vector(df["text"].to_list())

        # Filter columns
        df = df[list_fields_schema]

        df.to_csv("/tmp/entities.csv", index=False)

        logger.info("Building entities")
        entities = [[] for _ in range(df.columns.size)]

        for _, row in df.iterrows():
            for i, field in enumerate(list_fields_schema):
                entities[i].append(row[field])

        logger.info("End build embeddings")

        return entities

    def add_docs_to_milvus(self, docs, embeddings, collection_name, file_path):
        """
        Store embedding vectors for docs int Milvus DB, under collection_name.
        Return vector_db if successful, otherwise return None
        """
        pass

    def get_embedding(self):
        self.embedding = HuggingFaceEmbeddings(model_name=self.model_name, model_kwargs={"device": EMBEDDING_DEVICE})
        return self.embedding

    def get_data_vector(self, data):
        emb = self.embedding.embed_documents(data)
        return emb
