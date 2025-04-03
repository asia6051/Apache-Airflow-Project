import datetime
import logging
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

from PostgreSQLCountRows import PostgreSQLCountRowsOperator


def set_params(**context):
    ti = context['ti']
    user_name = ti.xcom_pull(task_ids='get_current_user')
    custom_id = uuid.uuid4().int % 123456789
    return {
        'custom_id': custom_id,
        'user_name': user_name,
        'timestamp': context['ts']
    }

def choose_next_task(**context):
    ti = context['ti']
    table_exists = ti.xcom_pull(task_ids='check_table_exists')

    if table_exists[0][0] is True:  # EXISTS() return list
        return 'insert_row'
    else:
        return 'create_table'


def insert_row(**context):
    ti = context['ti']
    user_name = ti.xcom_pull(task_ids='get_current_user')
    custom_id = uuid.uuid4().int % 123456789
    timestamp = context['ts']

    sql = f"""
    INSERT INTO table_1 (custom_id, user_name, timestamp)
    VALUES ({custom_id}, '{user_name}', '{timestamp}'
    );
    """

    PostgresOperator(
        task_id='insert_row',
        postgres_conn_id='postgres_conn',
        sql=sql
    ).execute(ti)

def push_function(**context):
    run_id = context['run_id']
    value_to_push = f"{ run_id } ended"
    Variable.set(key='result', value=value_to_push)


config = {
    'dag_id_1': {
        'start_date': datetime.datetime(2024, 8, 23),
        'schedule_interval': None,
        'table_name': 'table_1'
    }
}

logging.getLogger().setLevel(logging.INFO)


with DAG(
    dag_id='dag_id_1',
    start_date=datetime.datetime(2024, 8, 23),
    catchup=False,
    schedule=None
) as dag:

    start = PythonOperator(
        task_id="print_process_start",
        python_callable=lambda: logging.info(f"{dag.dag_id} start processing tables")
    )

    get_user = BashOperator(
        task_id='get_current_user',
        bash_command='whoami',
        do_xcom_push=True
    )

    check_table = PostgresOperator(
        task_id='check_table_exists',
        postgres_conn_id='postgres_conn',
        sql="""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'table_1'
        );
        """,
        do_xcom_push=True
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_conn',
        sql="""
        CREATE TABLE table_1(
            custom_id integer NOT NULL, 
            user_name VARCHAR (50) NOT NULL, 
            timestamp TIMESTAMP NOT NULL
        ); 
        """
    )

    insert_row = PythonOperator(
        task_id='insert_row',
        python_callable=insert_row
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_next_task,
        provide_context=True
    )

    query_table = PostgreSQLCountRowsOperator(
        task_id="query_table",
        sql_conn_id='postgres_conn',
        table_name='table_1',
        trigger_rule='none_failed'
    )


    start >> get_user >> check_table >> branch_task
    branch_task >> create_table >> query_table
    branch_task >> insert_row >> query_table
