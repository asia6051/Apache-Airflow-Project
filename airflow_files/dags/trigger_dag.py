from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from slack import WebClient
from slack.errors import SlackApiError
from datetime import datetime
from MyFileSensor import MyFileSensor


def pull_function(**context):
    value_pulled = Variable.get('result', default_var=None)
    print(f'Row count pulled from other DAG: {value_pulled}')


def slack_message(**context):
    slack_token = Variable.get('slack_token')
    if slack_token:
        client = WebClient(token=slack_token)

        dag_id = context['ti'].dag_id
        execution_date = context['ts']

        message = f"DAG ID: {dag_id}\n Execution date: {execution_date}"

        try:
            response = client.chat_postMessage(
                channel='#airflow-test',
                text=message
            )
        except SlackApiError as e:
            print(f"Error sending message: {e.response['error']}")

with DAG(
    dag_id="trigger_dag",
    start_date=datetime(2024, 8, 23),
    catchup=False,
    schedule=None
) as dag:

    wait_for_file = MyFileSensor(
        task_id='sensor_wait_run_file',
        filepath=Variable.get('name_path_variable', default_var='run'),
        fs_conn_id='my_file_system',
        poke_interval=5,
        timeout=60,
        mode='poke'
    )

    trigger_dag = TriggerDagRunOperator(
        task_id='Trigger_DAG',
        trigger_dag_id='dag_id_1',
        wait_for_completion=True
    )

    with TaskGroup(group_id='task_group_manage_results') as task_group_results:

        print_results = PythonOperator(
            task_id='print_result',
            python_callable=pull_function
        )
        remove_file = BashOperator(
            task_id='Remove_run_file',
            bash_command='rm /tmp/test.txt'
        )

        create_timestamp = BashOperator(
            task_id='create_finished_timestamp',
            bash_command='touch /tmp/finished_{{ ts_nodash }}'
        )
        print_results >> remove_file >> create_timestamp


    alert_slack = PythonOperator(
        task_id='Alert_slack',
        python_callable=slack_message,
        provide_context=True
    )

    wait_for_file >> trigger_dag >> task_group_results >> alert_slack
