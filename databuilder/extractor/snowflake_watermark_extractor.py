# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import Iterator, Union

from pyhocon import ConfigFactory, ConfigTree

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.watermark import Watermark


class SnowflakeWatermarkExtractor(Extractor):
    """
    Note: Experimental extractor for snowflake watermarks.
    Extracts Watermarks for tables which contain date columns ("DATE" case insensitive)
    Requirements:
        snowflake-connector-python
        snowflake-sqlalchemy
    """

    DATABASES = ['database_key']
    OI_COLUMN_NAME = ' '

    def init(self, conf: ConfigTree) -> None:
        self.sql_stmt = self.prepare_sql_statement(conf)

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope()) \
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter: Union[None, Iterator] = None

    def extract(self) -> Union[Watermark, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.snowflake_watermark'

    def _get_extract_iter(self) -> Iterator[Watermark]:
        """
        Provides iterator of result row from SQLAlchemy extractor
        """
        tbl_watermark_row = self._alchemy_extractor.extract()
        while tbl_watermark_row:
            cluster = tbl_watermark_row['database']
            database = 'snowflake'

            yield Watermark(
                create_time='2020-01-01 12:12:12',
                database=database,
                schema=tbl_watermark_row['table_schema'],
                table_name=tbl_watermark_row['table_name'],
                part_name=f'date={tbl_watermark_row["min_date"]}',
                part_type="low_watermark",
                cluster=cluster
            )

            yield Watermark(
                create_time='2020-01-01 12:12:12',
                database=database,
                schema=tbl_watermark_row['table_schema'],
                table_name=tbl_watermark_row['table_name'],
                part_name=f'date={tbl_watermark_row["max_date"]}',
                part_type="high_watermark",
                cluster=cluster,
            )
            tbl_watermark_row = self._alchemy_extractor.extract()

    def prepare_sql_statement(self, conf):
        # TODO: remove custom filter criterias and parameterize
        prep_statement = """
            select column_name, table_name, table_schema, table_catalog
            from SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
            where data_type in ('DATE', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ')
            and column_name ILIKE ANY ('{date_column_name}')
            and table_name not in  ('INTERNATIONAL_REACH_MONTHLY')
            and table_catalog in ('DATAHUB')
            and deleted is null
            """

        # if self.DATABASES:
        #     prep_statement += "and table_catalog in ('DATAHUB')"

        prepsts = prep_statement.format(
            date_column_name='date'
        )

        alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, alchemy_extractor.get_scope()) \
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: prepsts}))

        alchemy_extractor.init(sql_alch_conf)

        sql_statement = ''

        row = alchemy_extractor.extract()
        while row:
            sql_statement += f"""
            select
              lower(\'{row['table_name']}\') as table_name,
              lower(\'{row['table_schema']}\') as table_schema,
              lower(\'{row['table_catalog']}\') as database,
              min(\"{row['column_name']}\") as min_date,
              max(\"{row['column_name']}\") as max_date
            from \"{row['table_catalog']}\".\"{row['table_schema']}\".\"{row['table_name']}\"
            """
            row = alchemy_extractor.extract()
            if row:
                sql_statement += '\n union all \n'

        return sql_statement
