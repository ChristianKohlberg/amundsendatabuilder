# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
Example script for extracting information about last update field of tables in Snowflake.
It extracts the information from information_schema.tables. The "last altered" column is
updated when the table gets an update (DML and DDL).
"""

from pyhocon import ConfigFactory

from databuilder.extractor.snowflake_table_last_updated_extractor import SnowflakeTableLastUpdatedExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor

# reuse variables and methods from table extract
from example.scripts.sample_snowflake_data_loader import connection_string, \
    NEO4j_PASSWORD, NEO4J_USER, NEO4J_ENDPOINT, DATABASE_NAME, WHERE_CLAUSE


def create_snowflake_table_last_update_job():
    tmp_folder = f'/var/tmp/amundsen/snowflake_table_last_update'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    sql_extractor = SnowflakeTableLastUpdatedExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=sql_extractor, loader=csv_loader)

    job_config = ConfigFactory.from_dict({
        'extractor.snowflake_table_last_updated.{}'.format(SnowflakeTableLastUpdatedExtractor.SNOWFLAKE_DATABASE_KEY): DATABASE_NAME,
        'extractor.snowflake_table_last_updated.{}'.format(SnowflakeTableLastUpdatedExtractor.WHERE_CLAUSE_SUFFIX_KEY): WHERE_CLAUSE,
        'extractor.snowflake_table_last_updated.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string(DATABASE_NAME),

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
    job = create_snowflake_table_last_update_job()
    job.launch()
    print('done!')
