"""
    Module 19 task from Airflow Course

    Create ETL DAG consisting of three steps:
    1. Download data from https://drive.google.com/file/d/1mpOl7MvbXUuzBr9BV_M5LiEWCpsnd2cu/view
            (this dataset represents a collection of car accident)

    2. Count the number of accidents per year.

    3. Print results to the console.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine


def download_data(**context):
    df = pd.read_csv('other_files/monroe-county-crash.csv', encoding='windows-1252')
    return df


def count_accidents_per_year(**context):
    df = context['ti'].xcom_pull(task_ids='download_data')

    db_uri = f'postgresql+psycopg2://airflow:airflow@airflow_files-postgres-1:5432/airflow'
    engine = create_engine(db_uri)

    df.to_sql('crash_data', if_exists='replace', con=engine, index=False)

    query = """
    SELECT "Year", COUNT(*) as accident_count
    FROM crash_data
    GROUP BY "Year"
    ORDER BY "Year"
    """
    results = pd.read_sql_query(query, engine)

    return results


def print_results(**context):
    accidents_per_year = context['ti'].xcom_pull(task_ids='count_accidents_per_year')
    for year, count in accidents_per_year.items():
        print(f"Year: {year}, Accidents: {count}")


with DAG(
    dag_id='ETL_pipeline_Module_19',
    start_date=datetime(2024, 9, 18),
    catchup=False,
    schedule=None
) as dag:

    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
        provide_context=True
    )

    query_data = PythonOperator(
        task_id='count_accidents_per_year',
        python_callable=count_accidents_per_year,
        provide_context=True
    )

    print_results = PythonOperator(
        task_id='print_results',
        python_callable=print_results,
        provide_context=True
    )

    download_data >> query_data >> print_results

