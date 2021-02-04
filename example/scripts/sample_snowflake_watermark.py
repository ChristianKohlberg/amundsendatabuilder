# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is an example script for extracting watermarks for tables in snowflake.
It is different from Bigquery that it is not looking for partitioned tables, but instead looking
for a specific column name in each table, checking if its a datetime/timestamp column
and derives min and max calculations for each.
"""

import os

from pyhocon import ConfigFactory

from databuilder.extractor.snowflake_watermark_extractor import SnowflakeWatermarkExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor

from example.scripts.sample_snowflake_data_loader import NEO4j_PASSWORD, NEO4J_USER, NEO4J_ENDPOINT, connection_string

# TODO: improve database parameterization
DATABASE_KEY = os.getenv('SNOWFLAKE_DATABASE_KEY')
DATE_COLUMN_NAME = 'date'


def create_sample_watermark_snowflake_job():
    tmp_folder = f'/var/tmp/amundsen/snowflake_watermark'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    sql_extractor = SnowflakeWatermarkExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(
        extractor=sql_extractor,
        loader=csv_loader
    )

    job_config = ConfigFactory.from_dict({
        f'extractor.snowflake_watermark.{SnowflakeWatermarkExtractor.DATABASES}': DATABASE_KEY,
        f'extractor.snowflake_watermark.{SnowflakeWatermarkExtractor.OI_COLUMN_NAME}': DATE_COLUMN_NAME,
        f'extractor.snowflake_watermark.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connection_string(DATABASE_KEY),

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


if __name__ == "__main__":
    job = create_sample_watermark_snowflake_job()
    job.launch()
    print('done!')
