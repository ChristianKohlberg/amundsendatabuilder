# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
Example script which explains how to load metadata for tables/views from Snowflake into neo4j without using Airflow.
It extracts the information from the information_schema.tables view available in each database.

1. SnowflakeMetadataExtractor extracts table metadata to memory.
2. FsNeo4jCSVLoader dumps extract from memory to file.
3. neo4j_csv_publisher inserts from files to neo4j.
"""

import logging
import os
import uuid

from elasticsearch.client import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.snowflake_metadata_extractor import SnowflakeMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

# Configuration Start

# Neo4j
NEO4J_ENDPOINT = f'bolt://{os.getenv("NEO4J_HOST", "localhost")}:7687'
NEO4J_USER = 'neo4j'
NEO4j_PASSWORD = 'test'

# Elasticsearch
ES_HOST = os.getenv("ES_HOST", "localhost")

# Snowflake
DATABASE_NAME = os.getenv("SNOWFLAKE_DATABASE_KEY", "YourSnowflakeDbName")


def connection_string(database):
    # Refer this doc: https://docs.snowflake.com/en/user-guide/sqlalchemy.html#connection-parameters
    # for supported connection parameters and configurations
    # supply a snowflake uri via .env to avoid leaking your credentials
    user = 'SNOWFLAKE_USERNAME'
    password = 'SNOWFLAKE_PASSWORD'
    account = 'SNOWFLAKE_ACCOUNT'
    role = 'SNOWFLAKE_ROLE'
    warehouse = 'SNOWFLAKE_WAREHOUSE'

    snowflake_uri = os.getenv("SNOWFLAKE_URI", None)

    return snowflake_uri if snowflake_uri else \
        f'snowflake://{user}:{password}@{account}/{database}?warehouse={warehouse}&role={role}'


### Configuration end


def create_sample_snowflake_job():
    tmp_folder = '/var/tmp/amundsen/tables'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    sql_extractor = SnowflakeMetadataExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=sql_extractor, loader=csv_loader)

    job_config = ConfigFactory.from_dict({
        f'extractor.snowflake.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connection_string(DATABASE_NAME),
        f'extractor.snowflake.{SnowflakeMetadataExtractor.SNOWFLAKE_DATABASE_KEY}': DATABASE_NAME,

        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': True,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.FORCE_CREATE_DIR}': True,

        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': NEO4J_ENDPOINT,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': NEO4J_USER,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': NEO4j_PASSWORD,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag'
    })
    return DefaultJob(
        conf=job_config,
        task=task,
        publisher=Neo4jCsvPublisher()
    )


def create_es_publisher_sample_job(
    elasticsearch_index_alias='table_search_index',
    elasticsearch_doc_type_key='table',
    model_name='databuilder.models.table_elasticsearch_document.TableESDocument',
    cypher_query=None,
    elasticsearch_mapping=None
):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_search_index`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param cypher_query:               Query handed to the `Neo4jSearchDataExtractor` class, if None is given (default)
                                       it uses the `Table` query baked into the Extractor
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    elasticsearch_client = Elasticsearch([{'host': ES_HOST}])

    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())

    job_config = ConfigFactory.from_dict({
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.GRAPH_URL_CONFIG_KEY}': NEO4J_ENDPOINT,
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.MODEL_CLASS_CONFIG_KEY}': model_name,
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_USER}': NEO4J_USER,
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_PW}': NEO4j_PASSWORD,

        f'loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY}': extracted_search_data_path,
        f'loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY}': 'w',

        f'publisher.elasticsearch.{ElasticsearchPublisher.FILE_PATH_CONFIG_KEY}': extracted_search_data_path,
        f'publisher.elasticsearch.{ElasticsearchPublisher.FILE_MODE_CONFIG_KEY}': 'r',
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY}': elasticsearch_client,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY}': elasticsearch_new_index_key,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY}': elasticsearch_doc_type_key,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY}': elasticsearch_index_alias,
    })

    # only optionally add these keys, so need to dynamically `put` them
    if cypher_query:
        job_config.put(f'extractor.search_data.{Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY}',
                       cypher_query)
    if elasticsearch_mapping:
        job_config.put(f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY}',
                       elasticsearch_mapping)

    return DefaultJob(
        conf=job_config,
        task=task,
        publisher=ElasticsearchPublisher()
    )


if __name__ == "__main__":
    job_neo = create_sample_snowflake_job()
    job_neo.launch()

    job_es = create_es_publisher_sample_job(
        elasticsearch_index_alias='table_search_index',
        elasticsearch_doc_type_key='table',
        model_name='databuilder.models.table_elasticsearch_document.TableESDocument'
    )
    job_es.launch()
    print('done!')
