from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from get_company import read_json
from get_vacancy import find_vacancies
from get_skills import calc

default_args = {
    'owner': 'popov',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

logger = logging.getLogger(__name__)
src = PostgresHook(postgres_conn_id='postgres_default')
src_conn = src.get_conn()

def log(log_text):
    logger.info(f"{log_text} DAGа result_project")

with DAG(
    dag_id='result_project',
    default_args=default_args,
    description='DAG for search for TOP-10 key skills',
    start_date=datetime(2023, 7, 30, 8),
    schedule_interval='@daily'
) as dag:
    start = PythonOperator(
        task_id='start_DAG',
        python_callable=log,
        op_args=['Start']
    )
    download_file_egrul = BashOperator(
        task_id='download_file',
        bash_command="wget --timestamping --directory-prefix=/home/serg/ --continue https://ofdata.ru/open-data/download/egrul.json.zip"
    )
    parse_egrul = PythonOperator(
        task_id='parse',
        python_callable=read_json
    )
    create_telecom_table = PostgresOperator(
        task_id='create_table_telecom_companies',
        postgres_conn_id="postgres_default",
        sql="sql/telecom_schema.sql"
    )
    update_records_telecom = PostgresOperator(
        task_id = 'name_transformations_corp',
        postgres_conn_id = 'postgres_default',
        sql = "sql/update_records_telecom.sql"
    )
    download_vacancies = PythonOperator(
        task_id = 'download_list_vacancies',
        python_callable = find_vacancies
    )
    create_vacancy_table = PostgresOperator(
        task_id='create_table_vacancies',
        postgres_conn_id="postgres_default",
        sql="sql/vacancy_schema.sql"
    )
    update_records_vacancy = PostgresOperator(
        task_id = 'name_transformations_vac',
        postgres_conn_id = 'postgres_default',
        sql = "sql/update_records_vacancy.sql"
    )
    calc_key_skills = PythonOperator(
        task_id = 'calculate_key_skills',
        python_callable = calc
    )
    stop = PythonOperator(
        task_id='stop_DAG',
        python_callable=log,
        op_args=['Stop']
    )

    start >>download_file_egrul >>  parse_egrul >> update_records_telecom >> calc_key_skills >> stop
    start >> create_telecom_table >> update_records_telecom
    start >> download_vacancies >> update_records_vacancy >> calc_key_skills
    start >> create_vacancy_table >> update_records_vacancy