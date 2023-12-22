# spark-tunning-ml

Spark process optimization using [Retrieval-Augmented Generation (RAG)](https://research.ibm.com/blog/retrieval-augmented-generation-RAG). WIP

## Table of Contents
- [spark-tunning-ml](#spark-tunning-ml)
  - [Table of Contents](#table-of-contents)
  - [Requirements](#requirements)
  - [Creating test environment](#creating-test-environment)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Testing](#testing)
  - [SparkUI compatible versions](#sparkui-compatible-versions)
  - [License](#license)

## Requirements
- [python3.9+](https://www.python.org/downloads/)
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)`

## Creating test environment
- **[Apache Spark](https://spark.apache.org/)** is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.
```bash
# create spark cluster with 2 workers and histoy server
make run-scaled

# URLs access: 
# - SparkUI API: http://localhost:4040/api/v1
# - Master node: http://localhost:9090
# - History server: http://localhost:18080

# Launch Spark job
make submit app=data_analysis_book/chapter03/word_non_null.py
```

- **[Milvus](https://milvus.io/)** is an open-source vector database designed to manage and search large-scale vector data. Vector data refers to datasets where each data point is represented as a vector, often used in applications such as machine learning, image and video analysis, natural language processing, and other fields.
```bash
cd thirdparty/milvus-cluster
docker-compose up

# URL access: http://127.0.0.1:19530
```

- **[Attu](https://github.com/milvus-io/attu)** is an all-in-one milvus administration tool. With Attu, you can dramatically reduce the cost of managing milvus.
```bash
cd thirdparty/attu
docker-compose up

# URL access: http://localhost:8000
```


## Installation

```bash
# Create a virtual environment.
make virtualenv

# Install the project in dev mode.
make install
```

## Usage

```bash
# Luanch process
spark_tunning_ml --sparkui_api_url http://localhost:4040/api/v1
```
## Testing
```bash
make test
```

## SparkUI compatible versions
- spark 2.3.2 to 3.3.3

## License

MIT License