# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import Iterator, Union

from pyhocon import ConfigTree, ConfigFactory

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_column_usage import TableColumnUsage, ColumnReader


class SnowflakeTableUsageExtractor(Extractor):
    """
    Extractor for approximate Snowflake table usage derived from query results.
    It is best to inspect the sql statement and understand its limitations. We rely
    on absolute table reference to find tables being used within queries, which does
    not cover relative table references. If an email address is not available,
    it uses the user_name instead.

    Note: It used the ColumnReader/ColumnUsageModel which created counts for each column.
    But the current implementation only allows for table usage, indicated by a column with
    the name "*".
    """

    def init(self, conf: ConfigTree) -> None:
        self.sql_stmt = self.prepare_sql_statement()

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope()) \
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter = self._get_extract_iter()

    def _get_extract_iter(self) -> Iterator[TableColumnUsage]:
        row = self._alchemy_extractor.extract()

        while row:
            columns = [ColumnReader(
                database='snowflake',
                cluster=row['database_name'],  # TODO: class looks like it swaped database <> cluster
                schema=row['table_schema'],
                table=row['table_name'],
                column='*',
                user_email=row['user'],
                read_count=row['query_count']
            )]
            row = self._alchemy_extractor.extract()
            yield TableColumnUsage(col_readers=columns)

    def extract(self) -> Union[TableColumnUsage, None]:
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.snowflake_table_usage'

    def prepare_sql_statement(self):
        return f'''
        WITH query_history AS
                 (SELECT email,
                         user_name,
                         database_name,
                         query_text,
                         SCHEMA_NAME,
                         start_time
                  FROM snowflake.account_usage.query_history AS qh
                  LEFT JOIN snowflake.account_usage.users AS u ON qh.user_name = u.name
                  WHERE query_type = 'SELECT'
                    AND start_time > dateadd(DAY, -90, current_date())
                    AND execution_status = 'SUCCESS'
                  LIMIT 100
                 ),

             tbl_match AS
                 (SELECT qh.*,
                         table_catalog,
                         TABLE_NAME,
                         table_type,
                         table_schema
                  FROM snowflake.account_usage.tables AS t
                           LEFT JOIN query_history AS qh
                  WHERE contains(qh.query_text, t.table_catalog || '"."' || t.table_schema || '"."' || t.table_name)
                    AND table_catalog IS NOT NULL
                    AND table_schema != 'INFORMATION_SCHEMA'
                    AND deleted IS NULL
                 )

        SELECT CASE
                   WHEN email IS NULL THEN user_name
                   ELSE email
                   END              AS user,
               lower(TABLE_NAME)    AS table_name,
               lower(table_schema)  AS table_schema,
               lower(table_catalog) AS database_name,
               count(*)             AS query_count
        FROM tbl_match
        GROUP BY 1, 2, 3, 4;
        '''
