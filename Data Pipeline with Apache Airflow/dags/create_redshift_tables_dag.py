import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from udac_example_dag import default_args

with DAG('sparkify_create_redshift_tables_dag',
         start_date=datetime.datetime.now(),
         default_args=default_args) as dag:
    PostgresOperator(
        task_id='create_redshift_tables',
        dag=dag,
        postgres_conn_id='redshift',
        sql='create_tables.sql'
    )