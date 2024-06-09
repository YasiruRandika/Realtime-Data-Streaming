# Real-Time Data Streaming Project

## Introduction

This project serves as a guide to building an end-to-end data engineering pipeline. It covers each stage
from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Apache Airflow,
Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is containerized using Docker for ease
of deployment and scalability.

## System Architecture

### Components:

1. **Data Source**: We use randomuser.me API to generate random user data for our pipeline.
2. **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
3. **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
4. **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
5. **Apache Spark**: For data processing with its master and worker nodes.
6. **Cassandra**: Where the processed data will be stored.

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker

## Getting Started

To get started with this project, follow these steps:

1. Clone this repository to your local machine.
2. Set up your environment with Docker.
3. Configure Apache Airflow for orchestrating the pipeline.
4. Utilize Apache Kafka and Zookeeper for real-time data streaming.
5. Implement Apache Spark for data processing.
6. Store processed data in Cassandra and PostgreSQL databases.
