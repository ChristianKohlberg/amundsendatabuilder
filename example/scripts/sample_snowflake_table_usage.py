# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
Example script for extracting table query usage information from Snowflake.
SnowflakeTableUsageExtractor is agnostic to specific databases. It only
upserts the data if a table already exists in Amundsen and does not
create new tables on the fly. This makes it possible to query the results
for an entire account.

Note: Extractor needs privileged access to
1. snowflake.account_usage.query_history
2. snowflake.information_schema.tables
"""

from pyhocon import ConfigFactory

from databuilder.extractor.snowflake_table_usage_extractor import SnowflakeTableUsageExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from example.scripts.sample_snowflake_data_loader import NEO4j_PASSWORD, NEO4J_ENDPOINT, NEO4J_USER, DATABASE_NAME, \
    connection_string


def create_sample_table_query_usage_snowflake_job():
    tmp_folder = f'/var/tmp/amundsen/snowflake_table_usage'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    sql_extractor = SnowflakeTableUsageExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(
        extractor=sql_extractor,
        loader=csv_loader
    )

    job_config = ConfigFactory.from_dict({
        'extractor.snowflake_table_usage.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string(DATABASE_NAME),

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
    job1 = create_sample_table_query_usage_snowflake_job()
    job1.launch()
    print('done!')
