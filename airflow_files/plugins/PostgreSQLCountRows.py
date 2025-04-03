"""
    Custom operator needed for Module 14 - Airflow Course
"""
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


class PostgreSQLCountRowsOperator(BaseOperator):
    def __init__(self, sql_conn_id: str, table_name: str, **kwargs):
        super().__init__(**kwargs)
        self.sql_conn_id = sql_conn_id
        self.table_name = table_name

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.sql_conn_id)
        sql = f"SELECT COUNT(*) FROM {self.table_name}"
        result = hook.get_first(sql)
        row_count = result[0]
        print(f"Row count is: {row_count}")
        Variable.set(key='result', value=row_count)
        return row_count
